package Data::FlexSerializer;
use Moose;
use Moose::Util::TypeConstraints qw(enum);
use MooseX::Types::Moose qw(Maybe Bool Int Str Object);
use MooseX::Types -declare => [ qw(
    DataFlexSerializerOutputFormats
) ];
use autodie;

our $VERSION = '1.08';

# Get the DEBUG constant from $Data::FlexSerializer::DEBUG or
# $ENV{DATA_FLEXSERIALIZER_DEBUG}
use Constant::FromGlobal DEBUG => { int => 1, default => 0, env => 1 };

use List::Util qw(min);
use Storable qw();
use JSON::XS qw();
use Sereal::Decoder qw();
use Sereal::Encoder qw();
use Compress::Zlib qw(Z_DEFAULT_COMPRESSION);
use IO::Uncompress::AnyInflate qw();
use Carp ();
use Data::Dumper qw(Dumper);

has assume_compression => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);

has detect_compression => (
    is      => 'ro',
    isa     => Bool,
    default => 0,
);

has compress_output => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);

has compression_level => (
    is      => 'ro',
    isa     => Maybe[Int],
);

has detect_storable => (
    is      => 'ro',
    isa     => Bool,
    default => 0,
);

has detect_sereal => (
    is      => 'ro',
    isa     => Bool,
    default => 0,
);

has detect_json => (
    is      => 'ro',
    isa     => Bool,
    default => 1,
);

enum DataFlexSerializerOutputFormats, [ qw(
    storable
    json
    sereal
) ];

coerce DataFlexSerializerOutputFormats,
    from Str,
    via { lc $_ };

has output_format => (
    is      => 'rw',
    isa     => DataFlexSerializerOutputFormats,
    default => 'json',
    coerce  => 1,
);

has sereal_encoder => (
    is         => 'ro',
    isa        => Object,
    lazy_build => 1,
);

sub _build_sereal_encoder { Sereal::Encoder->new }

has sereal_decoder => (
    is         => 'ro',
    isa        => Object,
    lazy_build => 1,
);

sub _build_sereal_decoder { Sereal::Decoder->new }

my %serializer = (
    json     => 'JSON::XS::encode_json($_)',
    storable => 'Storable::nfreeze($_)',
    sereal   => '$self->{sereal_encoder}->encode($_)',
);

my %detector = (
    json     => '/^(?:\{|\[)/',
    storable => 's/^pst0//', # this is not a real detector.
                             # It just removes the storable
                             # file magic if necessary.
                             # Tho' storable needs to be last
    sereal   => '$self->{sereal_decoder}->looks_like_sereal($_)',
);

my %deserializer = (
    json     => 'JSON::XS::decode_json($_)',
    storable => 'Storable::thaw($_)',
    sereal   => 'do { my $structure; $self->{sereal_decoder}->decode($_, $structure); $structure }',
);

# no need to inialize anything for the default formats
my %initializer;

# Storable needs to be always the last as we
# don't have a nice way to detect it
my @detectors_order = qw/sereal json storable/;

sub add_serializer {
    my ($self, $name, $config) = @_;
    if ((my $serialize = $config->{serialize})) {
        warn "$name serializer overriden"
          if exists $serializer{$name};
        $serializer{$name} = $serialize;
    }
    if ((my $detect = $config->{detect})) {
        warn "$name detector overriden"
          if exists $detector{$name};
        $detector{$name} = $detect;
        unshift @detectors_order, $name;
    }
    if (my $deserialize = $config->{deserialize}) {
        warn "$name deserializer overriden"
          if exists $deserializer{$name};
        $deserializer{$name} = $deserialize;
    }
    if (my $initialize = $config->{initialize}) {
        die "The initializer needs to be a code ref. We got: $initialize"
          unless ref $initialize eq 'CODE';
        warn "$name deserializer overriden"
          if exists $initializer{$name};
        $initializer{$name} = $initialize;
    }
    return;
}

around BUILDARGS => sub {
    my ( $orig, $class, %args ) = @_;

    # We change the default on assume_compression to "off" if the
    # user sets detect_compression explicitly
    if (exists $args{detect_compression} and
        not exists $args{assume_compression}) {
        $args{assume_compression} = 0;
    }

    if ($args{assume_compression} and $args{detect_compression}) {
        die "Can't assume compression and auto-detect compression at the same time. That makes no sense.";
    }

    my $rv = $class->$orig(%args);

    if (DEBUG) {
        warn "Dumping the new FlexSerializer object.\n" . Dumper($rv);
    }

    return $rv;
};

sub BUILD {
    my ($self) = @_;

    # We may or may not have Sereal::{Decoder,Encoder} objects, if not
    # build them
    $self->sereal_decoder if $self->detect_sereal;
    $self->sereal_encoder if $self->output_format eq 'sereal';

    my @detect = grep { my $meth = "detect_$_"; $self->$meth } @detectors_order;

    if (!@detect) {
        die "Can't deserialize without assuming or detecting a format. Pass a detect_{format} option. If only one is passed we don't do detection and always assume to receive that format.";
    }

    $self->{serializer_coderef}   = $self->make_serializer;
    $self->{deserializer_coderef} = $self->make_deserializer;

    return;
}

sub serialize   { goto $_[0]->{serializer_coderef} }
sub deserialize { goto $_[0]->{deserializer_coderef} }

sub make_serializer {
    my $self = shift;
    my $compress_output = $self->compress_output;
    my $output_format = $self->output_format;
    my $comp_level;
    $comp_level = $self->compression_level if $compress_output;

    if (DEBUG) {
        warn(sprintf(
            "FlexSerializer using the following options for serialization: "
            . "compress_output=%s, compression_level=%s, output_format=%s",
            map {defined $self->{$_} ? $self->{$_} : '<undef>'}
            qw(compress_output compression_level output_format)
        ));
    }

    my $serializer = $serializer{$output_format}
        or die "PANIC: unknown output format '$output_format'";

    my $code;
    if ($compress_output) {
        my $comp_level_code = defined $comp_level ? $comp_level : 'Z_DEFAULT_COMPRESSION';
        $code = "Compress::Zlib::compress(\\$serializer,$comp_level_code)";
    } else {
        $code = $serializer;
    }

    $code = sprintf q{
        sub {
          # local *__ANON__= "__ANON__serialize__";
          my $self = shift;

          my @out;
          push @out, %s for @_;

          return wantarray ? @out
               : @out >  1 ? die( sprintf "You have %%d serialized structures, please call this method in list context", scalar @out )
               :            $out[0];

          return @out;
        };
    }, $code;

    warn $code if DEBUG >= 2;

    # Some serializers may need initialization
    exists $initializer{$output_format} and $initializer{$output_format}();

    my $coderef = eval $code or do{
        my $error = $@ || 'Zombie error';
        die "Couldn't create the deserialization coderef: $error\n The code is: $code\n";
    };

    return $coderef;
}

sub make_deserializer {
    my $self = shift;

    my $assume_compression = $self->assume_compression;
    my $detect_compression = $self->detect_compression;

    my @detectors = grep { my $meth = "detect_$_"; $self->$meth } @detectors_order;

    if (DEBUG) {
        warn "Detectors: @detectors";
        warn("FlexSerializer using the following options for deserialization: ",
            join ', ', map {defined $self->$_ ? "$_=@{[$self->$_]}" : "$_=<undef>"}
            qw(assume_compression detect_compression), map { "detect_$_" } @detectors
        );
    }

    my $uncompress_code;
    if ($assume_compression) {
        $uncompress_code = '
        local $_ = Compress::Zlib::uncompress(\$serialized);
        unless (defined $_) {
            die "You\'ve told me to assume compression but calling uncompress() on your input string returns undef";
        }';
    }
    elsif ($detect_compression) {
        $uncompress_code = '
        local $_;
        my $inflatedok = IO::Uncompress::AnyInflate::anyinflate(\$serialized => \$_);
        warn "FlexSerializer: Detected that the input was " . ($inflatedok ? "" : "not ") . "compressed"
            if DEBUG >= 3;
        $_ = $serialized if not $inflatedok;';
    }
    else {
        warn "FlexSerializer: Not using compression" if DEBUG;
        $uncompress_code = '
        local $_ = $serialized;';
    }

    my $code_detect = q!
        warn "FlexSerializer: %2$s that the input was %1$s" if DEBUG >= 3;
        warn sprintf "FlexSerializer: This was the %1$s input: '%s'",
            substr($_, 0, min(length($_), 100)) if DEBUG >= 3;
        push @out, !;

    my $code = @detectors == 1
        # Just one detector => skip the if()else gobbledigook
        ? sprintf $code_detect . $deserializer{$detectors[0]} . ";\n", $detectors[0], 'Assuming'
        # Multiple detectors
        : join('', map {
              my $body = $code_detect . $deserializer{$detectors[$_]} . ";\n";
              sprintf(
                  ($_ == 0           ? "if ( $detector{$detectors[$_]} ) {\n$body\n    }"
                  :$_ == $#detectors ? " else { $detector{$detectors[$_]};\n$body\n    }"
                  :                    " elsif ( $detector{$detectors[$_]} ) {\n$body\n    }"),
                  $detectors[$_],
                  ($_ == $#detectors ? 'Assuming' : 'Detected'),
              );
          } 0..$#detectors
        );

    $code = sprintf(q{
        sub {
          # local *__ANON__= "__ANON__deserialize__";
          my $self = shift;

          my @out;
          for my $serialized (@_) {
            %s

            %s
          }

          return wantarray ? @out
               : @out >  1 ? die( sprintf "You have %%d deserialized structures, please call this method in list context", scalar @out )
               :            $out[0];

          return @out;
        };},
        $uncompress_code, $code
    );

    warn $code if DEBUG >= 2;

    # Some serializers may need initialization
    exists $initializer{$_} and $initializer{$_}() for @detectors;

    my $coderef = eval $code or do{
        my $error = $@ || 'Clobbed';
        die "Couldn't create the deserialization coderef: $error\n The code is: $code\n";
    };

    return $coderef;
}

sub deserialize_from_file {
    my $self = shift;
    my $file = shift;

    if (not defined $file or not -r $file) {
        Carp::croak("Need filename argument or can't read file");
    }

    open my $fh, '<', $file;
    local $/;
    my $data = <$fh>;
    my ($rv) = $self->deserialize($data);
    return $rv;
}

sub serialize_to_file {
    my $self = shift;
    my $data = shift;
    my $file = shift;

    if (not defined $file) {
        Carp::croak("Need filename argument");
    }

    open my $fh, '>', $file;
    print $fh $self->serialize($data);
    close $fh;

    return 1;
}

sub deserialize_from_fh {
    my $self = shift;
    my $fd = shift;

    if (not defined $fd) {
        Carp::croak("Need file descriptor argument");
    }

    local $/;
    my $data = <$fd>;
    my ($rv) = $self->deserialize($data);

    return $rv;
}

sub serialize_to_fh {
    my $self = shift;
    my $data = shift;
    my $fd = shift;

    if (not defined $fd) {
        Carp::croak("Need file descriptor argument");
    }

    print $fd $self->serialize($data);

    return 1;
}


1;

__END__

=pod

=encoding utf8

=head1 NAME

Data::FlexSerializer - (De-)serialization from/to (compressed) JSON, Storable or Sereal

=head1 SYNOPSIS

This module was originally written to convert away from persistent use
of Storable to using JSON at Booking.com. Since then mostly due to
various issues with JSON not accurately being able to represent Perl
datastructures (e.g. preserve encoding flags) we've started to migrate
to L<Sereal::Encoder|Sereal> instead.

However the API of this module is now slightly awkward because now it
needs to deal with the possible detection and emission of these three
formats, and it still uses the JSON format by default which is no
longer the recommended way to use it.

  # For all of the below
  use Data::FlexSerializer;

=head2 Reading and writing compressed JSON

  # We *only* read/write compressed JSON by default:
  my $strict_serializer = Data::FlexSerializer->new;
  my @blobs = $strict_serializer->serialize(@perl_datastructures);
  my @perl_datastructures = $strict_serializer->deserialize(@blobs);

=head2 Reading maybe compressed JSON and writing compressed JSON

  # We can optionally detect compressed JSON as well, will accept
  # mixed compressed/uncompressed data. This works for all the input
  # formats.
  my $lax_serializer = Data::FlexSerializer->new(
    detect_compression => 1,
  );

=head2 Reading definitely compressed JSON and writing compressed JSON

  # If we know that all our data is compressed we can skip the
  # detection step. This works for all the input formats.
  my $lax_compress = Data::FlexSerializer->new(
    assume_compression => 1,
    compress_output => 1, # This is the default
  );

=head2 Migrate from maybe compressed Storable to compressed JSON

  my $storable_to_json = Data::FlexSerializer->new(
    detect_compression => 1, # check whether the input is compressed
    detect_storable => 1, # accept Storable images as input
    compress_output => 1, # This is the default
  );

=head2 Migrate from maybe compressed JSON to Sereal

  my $storable_to_sereal = Data::FlexSerializer->new(
    detect_sereal => 1,
    output_format => 'sereal',
  );

=head2 Migrate date from Sereal to JSON

  my $sereal_backcompat = Data::FlexSerializer->new(
    detect_sereal => 1, # accept Sereal images as input
  );

=head2 Migrate from JSON OR Storable to Sereal

  my $flex_to_json = Data::FlexSerializer->new(
    detect_compression => 1,
    detect_sereal => 1,
    detect_storable => 1,
    output_format => 'sereal',
  );

=head2 Migrate from JSON OR Storable to Sereal with custom Sereal objects

  my $flex_to_json = Data::FlexSerializer->new(
    detect_compression => 1,
    detect_sereal => 1,
    detect_storable => 1,
    output_format => 'sereal',
    sereal_decoder => Sereal::Decoder->new(...),
    sereal_encoder => Sereal::Encoder->new(...),
  );

=head1 DESCRIPTION

This simple OO module implements a serializer/deserializer for the
basic builtin Perl data structures (no blessed structures, no
filehandles, no regexes, no self-referential structures). It can
produce JSON, Storable or Sereal images. The output can be
zlib-compressed or uncompressed depending on settings.

The deserialization phase is more powerful: Depending on the serializer
settings, it can be strict in only accepting compressed or uncompressed
JSON, or it can auto-detect zlib compression. Additionally, since the
purpose of this is to allow painless migration away from storing
Storable images persistently, the deserialization code will optionally
detect that the input data is a (compressed or uncompressed) Storable
image and handle it gracefully. This flexibility comes at a price in
performance, so in order to keep the impact low, the default options
are more restrictive, see below.

=head1 METHODS

=head2 new

Constructor. Takes named arguments.

=head3 assume_compression

C<assume_compression> is a flag that makes the deserialization assume
that the data will be compressed. It won't have to guess, making the
deserialization faster. Defaults to true.

=head3 detect_compression

C<detect_compression> is a flag that also affects only the deserialization
step. If set, it'll auto-detect whether the input is compressed. Mutually
exclusive with C<assume_compression>. If C<detect_compression> is set, but
C<assume_compression> is not explicitly specified, C<assume_compression> will
be disabled (where it otherwise defaults to true).

=head3 compress_output

C<compress_output> is a flag indicating whether compressed or uncompressed
dumps are to be generated during the serialization. Defaults to true.

=head3 compression_level

C<compression_level> is an integer indicating the compression level (0-9).

=head3 output_format

C<output_format> can be either set to the string C<json> (default),
C<storable> or C<sereal>.  It has the obvious effect. Its value can be
changed at runtime via the accessor to facilitate having certain
output formats in experiments.

Note that if you dynamically change this to C<sereal> at runtime the
first call to L</serialize> after that will dynamically construct a
L<Sereal::Encoder> object, to avoid this supply a custom
L</sereal_encoder> object when constructing the object, and we won't
have to construct it dynamically later.

=head3 detect_json

C<detect_json>, if set, forces C<Data::FlexSerializer> into
JSON-compatibility mode. Defaults to on. 

=head3 detect_storable

C<detect_storable>, if set, forces C<Data::FlexSerializer> into
Storable-compatibility mode. Apart from JSON input, it will also detect whether
the provided blob is in valid Storable format. Defaults to off.

=head3 detect_sereal

C<detect_sereal>, if set, forces C<Data::FlexSerializer> into
Sereal-compatibility mode. Apart from JSON input, it will also detect whether
the provided blob is in valid Sereal format. Defaults to off.

=head3 sereal_encoder

=head3 sereal_decoder

You can supply C<sereal_encoder> or C<sereal_decoder> arguments with
your own Serial decoder/encoder objects. Handy if you want to pass
custom options to the encoder or decoder.

By default we create objects for you at BUILD time. So you don't need
to supply this for optimization purposes either.

=head2 serialize

Given a list of things to serialize, this does the job on each of them and
returns a list of serialized blobs.

In scalar context, this will return a single serialized blob instead of a
list. If called in scalar context, but passed a list of things to serialize,
this will croak because the call makes no sense.

=head2 deserialize

The opposite of C<serialize>, doh.

=head2 deserialize_from_file

Given a (single!) file name, reads the file contents and deserializes them.
Returns the resulting Perl data structure.

Since this works on one file at a time, this doesn't return a list of
data structures like C<deserialize()> does.

=head2 serialize_to_file

  $serializer->serialize_to_file(
    $data_structure => '/tmp/foo/bar'
  );

Given a (single!) Perl data structure, and a (single!) file name,
serializes the data structure and writes the result to the given file.
Returns true on success, dies on failure.

=head1 AUTHOR

Steffen Mueller <smueller@cpan.org>

Ævar Arnfjörð Bjarmason <avar@cpan.org>

Burak Gürsoy <burak@cpan.org>

Elizabeth Matthijsen <liz@dijkmat.nl>

Caio Romão Costa Nascimento <cpan@caioromao.com>

Jonas Galhordas Duarte Alves <jgda@cpan.org>

=head1 ACKNOWLEDGMENT

This module was originally developed at and for booking.com.
With approval from booking.com, this module was generalized
and put on CPAN, for which the authors would like to express
their gratitude.

=head1 COPYRIGHT AND LICENSE

 (C) 2011, 2012, 2013 Steffen Mueller and others. All rights reserved.

 This code is available under the same license as Perl version
 5.8.1 or higher.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

=cut
