use strict;
use warnings;
use Test::More tests => 9;
use Data::FlexSerializer;
use Sereal::Encoder qw();
use Sereal::Decoder qw();
use Scalar::Util qw(blessed);

# Make sure we properly build Sereal objects at object construction time
{
  my $flex_enc = Data::FlexSerializer->new(output_format => 'sereal');
  ok(blessed($flex_enc->{sereal_encoder}), "We have a Sereal encoder object");
  ok(!exists $flex_enc->{sereal_decoder}, "No need for a decoder, we're only encoding");
  my $flex_dec = Data::FlexSerializer->new(detect_sereal => 1);
  ok(blessed($flex_dec->{sereal_decoder}), "We have a Sereal decoder object");
  ok(!exists $flex_dec->{sereal_encoder}, "No need for a decoder, we're only encoding");
  my $flex_both = Data::FlexSerializer->new(detect_sereal => 1, output_format => 'sereal');
  ok(blessed($flex_both->{sereal_decoder}), "We have a Sereal decoder object");
  ok(blessed($flex_both->{sereal_encoder}), "We have a Sereal encoder object ");
}

# Check that we can provide a custom objects
{
  my $encoder = Sereal::Encoder->new;
  my $decoder = Sereal::Decoder->new;
  my $flex_both = Data::FlexSerializer->new(
    detect_sereal => 1,
    output_format => 'sereal',
    sereal_encoder => $encoder,
    sereal_decoder => $decoder,
  );
  ok($encoder == $flex_both->{sereal_encoder}, "We use encoder objects passed to us");
  ok($decoder == $flex_both->{sereal_decoder}, "We use decoder objects passed to us");
}

# Check that we can dynamically change the output format
{
  my $flex_dynamic = Data::FlexSerializer->new(
    detect_sereal => 1,
  );
  $flex_dynamic->output_format('sereal');
  my $value = [];
  my $serialize = $flex_dynamic->serialize($value);
  my $deserialize = $flex_dynamic->deserialize($serialize);
  is_deeply($value, $deserialize, "We can dynamically change the output format");
}
