use 5.008001;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'Data::FlexSerializer',
    VERSION_FROM      => 'lib/Data/FlexSerializer.pm', # finds $VERSION
    PREREQ_PM         => {
      'Storable' => '0',
      'JSON::XS' => '0',
      'Any::Moose' => '0',
      'Moose' => '0',
      'List::Util' => '0',
      'Compress::Zlib' => '0',
      'IO::Uncompress::AnyInflate' => '0',
      'Carp' => '0',
      'Constant::FromGlobal' => '0',
    }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Data/FlexSerializer.pm', # retrieve abstract from module
       AUTHOR         => 'Steffen Mueller <smueller@cpan.org>') : ()),
);
