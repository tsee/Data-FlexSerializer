use strict;
use warnings;
use Test::More qw(no_plan);
use Data::FlexSerializer;
use Sereal::Encoder qw(encode_sereal);
use Sereal::Decoder qw(decode_sereal);
use Data::Dumper;

{
    my $flex = Data::FlexSerializer->new(
        assume_compression => 0,
        detect_compression => 0,
        output_format      => 'storable',
        detect_json        => 1,
        detect_storable    => 1,
        detect_sereal      => 0,
    );
    is_deeply($flex->detect_formats, { storable => 1, json => 1, sereal => 0 }, "We have the expected detection formats");
    is_deeply($flex->deserialize(qq[{"foo": "bar"}]), { foo => "bar" }, "We can detect JSON");
    $flex->detect_json(0);
    is_deeply($flex->detect_formats, { storable => 1, json => 0, sereal => 0 }, "We have the expected detection formats");
    eval {
        $flex->deserialize(qq[{"foo": "bar"}]);
        fail "Why does this work now?";
        1;
    } or do {
        chomp(my $error = $@ || "Zombie Error");
        pass "We shouldn't be able to deserialize JSON anymore: $error";
    };
    $flex->detect_storable(0);
    $flex->detect_sereal(1);
    is_deeply($flex->detect_formats, { storable => 0, json => 0, sereal => 1 }, "We have the expected detection formats");
    eval {
        is_deeply($flex->deserialize(encode_sereal({foo => "bar"})), { foo => "bar" }, "We can now deserialize Sereal");
        1;
    } or do {
        chomp(my $error = $@ || "Zombie Error");
        fail "We shouldn't die on this: $error";
    };
    $flex->detect_sereal(0);
    is_deeply($flex->detect_formats, { storable => 0, json => 0, sereal => 0 }, "We have the expected detection formats");
    eval {
        $flex->deserialize(qq[{"foo": "bar"}]);
        fail "We should die on this as we have no formats left";
        1;
    } or do {
        pass "We died because we had no formats left";
    };
}
