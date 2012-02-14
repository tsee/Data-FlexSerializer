use strict;
use warnings;
use autodie;
use Data::FlexSerializer;
use Storable qw/nfreeze thaw/;
use Compress::Zlib qw(Z_DEFAULT_COMPRESSION);
use JSON::XS qw/encode_json decode_json/;
use Test::More;

use constant DF => 'Data::FlexSerializer';

# Test default settings
my $default_szer = DF->new();
isa_ok($default_szer, 'Data::FlexSerializer');
my %defaults = (
  assume_compression => 1,
  detect_compression => 0,
  compress_output => 1,
  compression_level => undef,
  detect_storable => 0,
  output_format => 'json',
);
foreach my $setting (sort keys %defaults) {
  is($default_szer->$setting, $defaults{$setting}, "defaults for $setting");
}

# check whether assume_compression is turned off implicitly if detect_compression is set
SCOPE: {
  my %opt = %defaults;
  delete $opt{assume_compression};
  $opt{detect_compression} = 1;
  my $s = DF->new(%opt);
  ok(!$s->assume_compression, "detect_compression implies assume_compression==0");
}

my %opt_no_compress = (
  assume_compression => 0,
  detect_compression => 0,
  compress_output => 0,
);
my %opt_accept_compress = (
  assume_compression => 0,
  detect_compression => 1,
  compress_output => 0,
);
my %opt_flex_in = (
  %opt_accept_compress,
  detect_storable => 1,
);
my %opt_storable = (
  output_format => 'storable',
  detect_storable => 1,
);

my %serializers = (
  default => $default_szer,
  json_compress => $default_szer,
  json_no_compress => DF->new(%opt_no_compress),
  json_flex_compress => DF->new(%opt_accept_compress, compress_output => 1),
  flex_compress => DF->new(detect_storable => 1, compress_output => 1),
  flex_no_compress => DF->new(%opt_flex_in, %opt_no_compress),
  flex_flex_compress => DF->new(%opt_flex_in),
  s_compress => DF->new(%opt_storable),
  s_no_compress => DF->new(%opt_storable, %opt_no_compress),
  s_flex_compress => DF->new(%opt_storable, %opt_accept_compress, compress_output => 1),
);

isa_ok($serializers{$_}, 'Data::FlexSerializer', 'Serializer for "$_"') for sort keys %serializers;

my %data = (
  raw => {foo => 'bar', baz => [2, 3, 4]},
  garbage => 'asdkj2qdal2djalkd',
);
$data{storable} = nfreeze($data{raw});
$data{json} = encode_json($data{raw});
$data{comp_json} = Compress::Zlib::compress(\$data{json}, Z_DEFAULT_COMPRESSION);
$data{comp_storable} = Compress::Zlib::compress(\$data{storable}, Z_DEFAULT_COMPRESSION);
$data{comp_garbage} = Compress::Zlib::compress(\$data{garbage}, Z_DEFAULT_COMPRESSION); # dubious

# input is implied to be the raw data on serialization, output listed
my %results_serialize = (
  'default' => 'comp_json',
  'json_compress' => 'comp_json',
  'json_no_compress' => 'json',
  'json_flex_compress' => 'comp_json',
  'flex_compress' => 'comp_json',
  'flex_no_compress' => 'json',
  'flex_flex_compress' => 'json',
  's_compress' => 'comp_storable',
  's_no_compress' => 'storable',
  's_flex_compress' => 'comp_storable',
);
foreach my $s_name (sort keys %results_serialize) {
  my $serializer = $serializers{$s_name} or die;
  my $data = $data{raw}; # always raw input for serialization
  my $tname = "serialization with $s_name";
  my $res;
  my @res;
  eval {$res = $serializer->serialize($data); 1} && defined $res
    ? pass("$tname in scalar context does not crash and returns non-undef")
    : fail("$tname in scalar context throws exception or results in undef");
  
  eval {@res = $serializer->serialize($data); 1} && @res == 1 && defined($res[0])
    ? pass("$tname in array context does not crash and returns non-undef")
    : fail("$tname in array context throws exception or results in undef");

  is($res, $res[0], "$tname same result in array and scalar context");

  # test actual output
  my $expected_output = $data{ $results_serialize{$s_name} } or die;
  is($res, $expected_output, "$tname output as expected");

  # now assert that garbage throws exceptions
  ok(not eval {$res = $serializer->serialize($data{garbage}); 1});
}

# maps input => expected output for each serializer.
# \undef output means exception
my %results_deserialize = (
  'default' => {
    'comp_json' => 'raw',
    (map {$_ => \undef} qw(json storable comp_storable garbage)),
  },
  'json_compress' => {
    'comp_json' => 'raw',
    (map {$_ => \undef} qw(json storable comp_storable garbage)),
  },
  'json_no_compress' => {
    'json' => 'raw',
    (map {$_ => \undef} qw(comp_json storable comp_storable garbage)),
  },
  'json_flex_compress' => {
    (map {$_ => 'raw'} qw(comp_json json)),
    (map {$_ => \undef} qw(storable comp_storable garbage)),
  },
  'flex_compress' => {
    (map {$_ => 'raw'} qw(comp_json comp_storable)),
    (map {$_ => \undef} qw(storable json garbage)),
  },
  'flex_no_compress' => {
    (map {$_ => 'raw'} qw(json storable)),
    (map {$_ => \undef} qw(comp_json comp_storable garbage)),
  },
  'flex_flex_compress' => {
    (map {$_ => 'raw'} qw(json comp_json storable comp_storable)),
    (map {$_ => \undef} qw(garbage)),
  },
  's_compress' => {
    (map {$_ => 'raw'} qw(comp_json comp_storable)),
    (map {$_ => \undef} qw(json storable garbage)),
  },
  's_no_compress' => {
    (map {$_ => 'raw'} qw(json storable)),
    (map {$_ => \undef} qw(comp_json comp_storable garbage)),
  },
  's_flex_compress' => {
    (map {$_ => 'raw'} qw(comp_json comp_storable json storable)),
    (map {$_ => \undef} qw(garbage)),
  },
);

foreach my $s_name (sort keys %results_deserialize) {
  my $serializer = $serializers{$s_name} or die;
  my $testset = $results_deserialize{$s_name};

  foreach my $input_data_name (sort keys %$testset) {
    my $exp_out = $testset->{$input_data_name};
    my $input_data = $data{$input_data_name};
    my $tname = "deserialization with $s_name for input '$input_data_name'";

    my $res;
    my @res;
    if (ref $exp_out && not defined $$exp_out) { # exception expected
      ok(not eval {$res = $serializer->deserialize($input_data); 1} );
      ok(not eval {@res = $serializer->deserialize($input_data); 1} );
    }
    else {
      $exp_out = $data{$exp_out};
      eval {$res = $serializer->deserialize($input_data); 1} && defined $res
        ? pass("$tname in scalar context does not crash and returns non-undef")
        : fail("$tname in scalar context throws exception or results in undef");
      is_deeply($res, $exp_out, "$tname in scalar context yields correct result");

      eval {@res = $serializer->deserialize($input_data); 1} && @res == 1 && defined($res[0])
        ? pass("$tname in array context does not crash and returns non-undef")
        : fail("$tname in array context throws exception or results in undef");
      is_deeply($res[0], $exp_out, "$tname in array context yields correct result");
    }
  }
}

done_testing();

