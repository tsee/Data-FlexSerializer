package Data::FlexSerializer;
use Moose;
use Moose::Util::TypeConstraints qw(enum);
use MooseX::Types::Moose qw(Maybe Bool Int Str Object);
use MooseX::Types -declare => [ qw(
    DataFlexSerializerOutputFormats
) ];
use autodie;

our $VERSION = '1.06';

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

has sereal_encoder_object => (
    is         => 'ro',
    isa        => Object,
    lazy_build => 1,
);

sub _build_sereal_encoder_object { Sereal::Encoder->new }

has sereal_decoder_object => (
    is         => 'ro',
    isa        => Object,
    lazy_build => 1,
);

sub _build_sereal_decoder_object { Sereal::Decoder->new }

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
        print STDERR "Dumping the new FlexSerializer object.\n" . Dumper($rv);
    }

    return $rv;
};

sub BUILD {
    my ($self) = @_;

    # We may or may not have Sereal::{Decoder,Encoder} objects, if not
    # build them
    $self->sereal_decoder_object if $self->detect_sereal;
    $self->sereal_encoder_object if $self->output_format eq 'sereal';

    return;
}

sub serialize {
  my $self = shift;

  my $do_compress = $self->{compress_output}; # hot path, bypass accessor
  my $output_format = $self->{output_format}; # hot path, bypass accessor
  my $comp_level;
  $comp_level = $self->{compression_level} if $do_compress; # hot path, bypass accessor

  if (DEBUG) {
    print STDERR sprintf(
      "FlexSerializer using the following options for serialization: "
      . "compress_output=%s, compression_level=%s, output_format=%s\n",
      map {defined $self->{$_} ? $self->{$_} : '<undef>'}
      qw(compress_output compression_level output_format)
    );
  }

  my @out;
  foreach my $data (@_) {
    my $serialized = $output_format eq 'json'
                   ? JSON::XS::encode_json($data)
                   : $output_format eq 'storable'
                     ? Storable::nfreeze($data)
                     : $output_format eq 'sereal'
                       ? $self->{sereal_encoder_object}->encode($data)
                       : die "PANIC: unknown output format '$output_format'";
    if ($do_compress) {
      $serialized = Compress::Zlib::compress(\$serialized,
                                             (defined($comp_level) ? ($comp_level) : (Z_DEFAULT_COMPRESSION)));
    }
    push @out, $serialized;
  }

  return wantarray ? @out
       : @out >  1 ? die( sprintf 'You have %d serialized structures, please call this method in list context', scalar @out )
       :            $out[0];
}

sub deserialize {
  my $self = shift;

  my $do_uncompress = $self->{assume_compression}; # hot path, bypass accessor
  my $detect_compression = $self->{detect_compression}; # hot path, bypass accessor
  my $detect_storable = $self->{detect_storable}; # hot path, bypass accessor
  my $detect_sereal = $self->{detect_sereal}; # hot path, bypass accessor
  my $sereal_decoder_object = $detect_sereal ? $self->{sereal_decoder_object} : undef;

  if (DEBUG) {
    print STDERR sprintf(
      "FlexSerializer using the following options for deserialization: "
      . "assume_compression=%s, detect_compression=%s, detect_storable=%s, detect_sereal=%s\n",
      map {defined $self->{$_} ? $self->{$_} : '<undef>'}
      qw(assume_compression detect_compression detect_storable detect_sereal)
    );
  }

  my @out;
  foreach my $serialized (@_) {
    my $uncompr;
    if ($do_uncompress) {
      $uncompr = Compress::Zlib::uncompress(\$serialized);
      unless (defined $uncompr) {
        die "You've told me to assume compression but calling uncompress() on your input string returns undef";
      }
    }
    elsif ($detect_compression) {
      my $inflatedok = IO::Uncompress::AnyInflate::anyinflate(\$serialized => \$uncompr);
      print STDERR "FlexSerializer: Detected that the input was " . ($inflatedok ? "" : "not ") . "compressed\n"
        if DEBUG;
      if (not $inflatedok) {
        $uncompr = $serialized;
      }
    }
    else {
      $uncompr = $serialized;
    }

    # Copy-paste galore ahead for performance reasons. We can either
    # make it pruttah or fast.
    if ($detect_sereal) {
      if ($sereal_decoder_object->looks_like_sereal($uncompr)) {
        print STDERR "FlexSerializer: Detected that the input was Sereal\n" if DEBUG;
        print STDERR "FlexSerializer: This was the Sereal input: '%s'\n",
              substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
        my $structure;
        $sereal_decoder_object->decode($uncompr, $structure);
        push @out, $structure;
      }
      elsif ($detect_storable) {
        if ($uncompr =~ /^(?:\{|\[)/) {
          print STDERR "FlexSerializer: Detected that the input was JSON\n" if DEBUG;
          print STDERR "FlexSerializer: This was the start of the JSON input: '%s'\n",
                substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
          push @out, JSON::XS::decode_json($uncompr);
        }
        else {
        #elsif (defined Storable::read_magic(substr($uncompr, 0, 21))) {
          print STDERR "FlexSerializer: Detected that the input was Storable\n" if DEBUG;
          print STDERR "FlexSerializer: This was the Storable input: '%s'\n",
                substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
          $uncompr =~ s/^pst0//; # remove Storable file magic if necessary
          push @out, Storable::thaw($uncompr);
        }
      }
      else {
        print STDERR "FlexSerializer: Assuming that the input is JSON\n" if DEBUG;
        print STDERR "FlexSerializer: This was the JSON input: '%s'\n",
             substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
        push @out, JSON::XS::decode_json($uncompr);
      }
    } else {
      if ($detect_storable) {
        if ($uncompr =~ /^(?:\{|\[)/) {
          print STDERR "FlexSerializer: Detected that the input was JSON\n" if DEBUG;
          print STDERR "FlexSerializer: This was the start of the JSON input: '%s'\n",
                substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
          push @out, JSON::XS::decode_json($uncompr);
        }
        else {
        #elsif (defined Storable::read_magic(substr($uncompr, 0, 21))) {
          print STDERR "FlexSerializer: Detected that the input was Storable\n" if DEBUG;
          print STDERR "FlexSerializer: This was the Storable input: '%s'\n",
                substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
          $uncompr =~ s/^pst0//; # remove Storable file magic if necessary
          push @out, Storable::thaw($uncompr);
        }
      }
      else {
        print STDERR "FlexSerializer: Assuming that the input is JSON\n" if DEBUG;
        print STDERR "FlexSerializer: This was the JSON input: '%s'\n",
             substr($uncompr, 0, min(length($uncompr), 100)) if DEBUG >= 2;
        push @out, JSON::XS::decode_json($uncompr);
      }
    }
  }

  return wantarray ? @out
       : @out >  1 ? die( sprintf 'You have %d deserialized structures, please call this method in list context', scalar @out )
       :            $out[0];

  return @out;
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
  close $fh or die "Error closing file handle: $!";

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
  close $fh or die "Error closing file handle: $!";

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

Data::FlexSerializer - (De-)serialization from/to (compressed) JSON or Storable

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

C<assume_compression> is a flag that makes the deserialization assume
that the data will be compressed. It won't have to guess, making the
deserialization faster. Defaults to true.

C<detect_compression> is a flag that also affects only the deserialization
step. If set, it'll auto-detect whether the input is compressed. Mutually
exclusive with C<assume_compression>. If C<detect_compression> is set, but
C<assume_compression> is not explicitly specified, C<assume_compression> will
be disabled (where it otherwise defaults to true).

C<compress_output> is a flag indicating whether compressed or uncompressed
dumps are to be generated during the serialization. Defaults to true.

C<compression_level> is an integer indicating the compression level (0-9).

C<output_format> can be either set to the string C<json> (default),
C<storable> or C<sereal>.  It has the obvious effect. Its value can be
changed at runtime via the accessor to facilitate having certain
output formats in experiments.

C<detect_storable>, if set, forces C<Data::FlexSerializer> into
Storable-compatibility mode. Apart from JSON input, it will also detect whether
the provided blob is in valid Storable format. Defaults to off.

C<detect_sereal>, if set, forces C<Data::FlexSerializer> into
Sereal-compatibility mode. Apart from JSON input, it will also detect whether
the provided blob is in valid Sereal format. Defaults to off.

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

=head1 ACKNOWLEDGMENT

This module was originally developed at and for booking.com.
With approval from booking.com, this module was generalized
and put on CPAN, for which the authors would like to express
their gratitude.

=head1 COPYRIGHT AND LICENSE

 (C) 2011, 2012 Steffen Mueller and others. All rights reserved.

 This code is available under the same license as Perl version
 5.8.1 or higher.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.

=cut
