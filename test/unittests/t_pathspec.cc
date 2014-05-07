#include <gtest/gtest.h>
#include <string>

#include "../../cvmfs/pathspec/pathspec.h"


TEST(T_Pathspec, ParseSimpleRelative) {
  const Pathspec p1("foo");
  const Pathspec p2("foo/bar");
  const Pathspec p3("foo/bar.baz");
  const Pathspec p4("foo/bar/baz.foo");

  EXPECT_TRUE (p1.IsValid()); EXPECT_FALSE (p1.IsAbsolute());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute());
  EXPECT_TRUE (p4.IsValid()); EXPECT_FALSE (p4.IsAbsolute());
}


TEST(T_Pathspec, ParseSimpleAbsolute) {
  const Pathspec p1("/");
  const Pathspec p2("/foo");
  const Pathspec p3("/foo/bar");
  const Pathspec p4("/foo/bar.baz");
  const Pathspec p5("/foo/bar/baz.foo");

  EXPECT_FALSE (p1.IsValid()); EXPECT_TRUE (p1.IsAbsolute());
  EXPECT_TRUE  (p2.IsValid()); EXPECT_TRUE (p2.IsAbsolute());
  EXPECT_TRUE  (p3.IsValid()); EXPECT_TRUE (p3.IsAbsolute());
  EXPECT_TRUE  (p4.IsValid()); EXPECT_TRUE (p4.IsAbsolute());
  EXPECT_TRUE  (p5.IsValid()); EXPECT_TRUE (p5.IsAbsolute());
}


TEST(T_Pathspec, ParsePlaceholders) {
  const Pathspec p1("/hallo/??test/test");
  const Pathspec p2("foo?bar");
  const Pathspec p3("bar.???");
  const Pathspec p4("/foo/bar/ba?");
  const Pathspec p5("/fo?/b?r/?az");

  EXPECT_TRUE (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute());
  EXPECT_TRUE (p4.IsValid()); EXPECT_TRUE  (p4.IsAbsolute());
  EXPECT_TRUE (p5.IsValid()); EXPECT_TRUE  (p5.IsAbsolute());
}


TEST(T_Pathspec, ParseWildcards) {
  const Pathspec p1("/hallo/*/test");
  const Pathspec p2("foo/*bar");
  const Pathspec p3("bar.*");
  const Pathspec p4("/foo/b*r");
  const Pathspec p5("/foo/*/bar");

  EXPECT_TRUE (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute());
  EXPECT_TRUE (p4.IsValid()); EXPECT_TRUE  (p4.IsAbsolute());
  EXPECT_TRUE (p5.IsValid()); EXPECT_TRUE  (p5.IsAbsolute());
}


TEST(T_Pathspec, ParseEscapes) {
  const Pathspec p1("/hallo/te\\*st/test");
  const Pathspec p2("/foo/b\\?r/test");
  const Pathspec p3("moep\\\\test");
  const Pathspec p4("moep\\xtest");

  EXPECT_TRUE  (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute());
  EXPECT_TRUE  (p2.IsValid()); EXPECT_TRUE  (p2.IsAbsolute());
  EXPECT_TRUE  (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute());
  EXPECT_FALSE (p4.IsValid()); EXPECT_FALSE (p3.IsAbsolute());
}


TEST(T_Pathspec, MatchOnAbsolutePath) {
  const Pathspec p1("/hallo");
  const Pathspec p2("bar");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());

  EXPECT_FALSE (p1.IsMatching("hallo"));
  EXPECT_FALSE (p2.IsMatching("/bar"));
}


TEST(T_Pathspec, MatchSimple) {
  const Pathspec p1("/hallo/welt");
  const Pathspec p2("foo.bar");
  const Pathspec p3("foo/bar/baz.txt");
  const Pathspec p4("/foo");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());
  EXPECT_TRUE (p3.IsValid());
  EXPECT_TRUE (p4.IsValid());

  EXPECT_FALSE (p1.IsMatching(""));
  EXPECT_FALSE (p2.IsMatching(""));
  EXPECT_FALSE (p3.IsMatching(""));
  EXPECT_FALSE (p4.IsMatching(""));

  EXPECT_TRUE  (p1.IsMatching("/hallo/welt"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt/"));
  EXPECT_FALSE (p1.IsMatching("/hallo/wel"));
  EXPECT_FALSE (p1.IsMatching("/foo/hallo/welt"));
  EXPECT_FALSE (p1.IsMatching("hallo/welt"));
  EXPECT_FALSE (p1.IsMatching("/welt/hallo"));

  EXPECT_TRUE  (p2.IsMatching("foo.bar"));
  EXPECT_TRUE  (p2.IsMatching("foo.bar/"));
  EXPECT_FALSE (p2.IsMatching("moep/foo.bar"));
  EXPECT_FALSE (p2.IsMatching("moep/foo.bar/"));
  EXPECT_FALSE (p2.IsMatching("moep/foo.bar/baz"));
  EXPECT_FALSE (p2.IsMatching("foo.bar/baz"));
  EXPECT_FALSE (p2.IsMatching("bar.foo"));
  EXPECT_FALSE (p2.IsMatching("/foo.bar"));

  EXPECT_TRUE  (p3.IsMatching("foo/bar/baz.txt"));
  EXPECT_TRUE  (p3.IsMatching("foo/bar/baz.txt/"));
  EXPECT_FALSE (p3.IsMatching("foo/bar/baz.txt/moep"));
  EXPECT_FALSE (p3.IsMatching("/foo/bar/baz.txt"));
  EXPECT_FALSE (p3.IsMatching("/foo/baz.txt"));
  EXPECT_FALSE (p3.IsMatching("/bar/foo/baz.txt"));

  EXPECT_TRUE  (p4.IsMatching("/foo"));
  EXPECT_TRUE  (p4.IsMatching("/foo/"));
  EXPECT_FALSE (p4.IsMatching("foo"));
  EXPECT_FALSE (p4.IsMatching("foo/bar"));
}


TEST(T_Pathspec, MatchEscapeSequences) {
  const Pathspec p1("/hallo\\*/welt");
  const Pathspec p2("/hallo\\\\/welt");
  const Pathspec p3("/hallo\\?/welt");
  const Pathspec p4("/foo\\?bar/welt");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());
  EXPECT_TRUE (p3.IsValid());
  EXPECT_TRUE (p4.IsValid());

  EXPECT_TRUE  (p1.IsMatching("/hallo*/welt"));
  EXPECT_TRUE  (p1.IsMatching("/hallo*/welt/"));
  EXPECT_FALSE (p1.IsMatching("hallo*/welt"));
  EXPECT_FALSE (p1.IsMatching("/halloo/welt"));
  EXPECT_FALSE (p1.IsMatching("/hallooo/welt"));

  EXPECT_TRUE  (p2.IsMatching("/hallo\\/welt"));
  EXPECT_TRUE  (p2.IsMatching("/hallo\\/welt/"));
  EXPECT_FALSE (p2.IsMatching("hallo\\/welt/"));
  EXPECT_FALSE (p2.IsMatching("hallo\\\\/welt/"));

  EXPECT_TRUE  (p3.IsMatching("/hallo?/welt"));
  EXPECT_TRUE  (p3.IsMatching("/hallo?/welt/"));
  EXPECT_FALSE (p3.IsMatching("/hallo/welt"));
  EXPECT_FALSE (p3.IsMatching("/hall/welt"));
  EXPECT_FALSE (p3.IsMatching("hallo?/welt"));
  EXPECT_FALSE (p3.IsMatching("hallo?/welt/"));

  EXPECT_TRUE  (p4.IsMatching("/foo?bar/welt"));
  EXPECT_TRUE  (p4.IsMatching("/foo?bar/welt/"));
  EXPECT_FALSE (p4.IsMatching("/foobar/welt"));
  EXPECT_FALSE (p4.IsMatching("/fobar/welt"));
  EXPECT_FALSE (p4.IsMatching("foobar/welt"));
  EXPECT_FALSE (p4.IsMatching("foo?bar/welt"));
  EXPECT_FALSE (p4.IsMatching("fobar/welt"));
}


TEST(T_Pathspec, MatchRegexEdgeCases) {
  const Pathspec p1("/hallo.welt");
  const Pathspec p2("foo.bar\\?");
  const Pathspec p3("foo/bar/baz.\\*");
  const Pathspec p4("/foo[bar]");
  const Pathspec p5("foo(bar)");
  const Pathspec p6("/foo/bar{baz}");
  const Pathspec p7("foo/^bar");
  const Pathspec p8("foo/bar$");
  const Pathspec p9("/moep+test/foo");
  const Pathspec p10("moep\\\\dtest/foo");
  const Pathspec p11("/moep+/foo\\?/bar\\*/t[e]st/hallo.welt");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());
  EXPECT_TRUE (p3.IsValid());
  EXPECT_TRUE (p4.IsValid());
  EXPECT_TRUE (p5.IsValid());
  EXPECT_TRUE (p6.IsValid());
  EXPECT_TRUE (p7.IsValid());
  EXPECT_TRUE (p8.IsValid());
  EXPECT_TRUE (p9.IsValid());
  EXPECT_TRUE (p10.IsValid());
  EXPECT_TRUE (p11.IsValid());

  EXPECT_TRUE  (p1.IsMatching("/hallo.welt"));
  EXPECT_TRUE  (p1.IsMatching("/hallo.welt/"));
  EXPECT_FALSE (p1.IsMatching("/halloxwelt"));
  EXPECT_FALSE (p1.IsMatching("/hallo?welt"));
  EXPECT_FALSE (p1.IsMatching("/hallowelt"));

  EXPECT_TRUE  (p2.IsMatching("foo.bar?"));
  EXPECT_TRUE  (p2.IsMatching("foo.bar?/"));
  EXPECT_FALSE (p2.IsMatching("foo.barx"));
  EXPECT_FALSE (p2.IsMatching("foo.barx/"));
  EXPECT_FALSE (p2.IsMatching("foo.bar"));
  EXPECT_FALSE (p2.IsMatching("foo.bar/"));

  EXPECT_TRUE  (p3.IsMatching("foo/bar/baz.*"));
  EXPECT_TRUE  (p3.IsMatching("foo/bar/baz.*/"));
  EXPECT_FALSE (p3.IsMatching("foo/bar/baz.txt"));
  EXPECT_FALSE (p3.IsMatching("foo/bar/baz.x/"));
  EXPECT_FALSE (p3.IsMatching("foo/bar/baz."));
  EXPECT_FALSE (p3.IsMatching("foo/bar/bazx*"));

  EXPECT_TRUE  (p4.IsMatching("/foo[bar]"));
  EXPECT_TRUE  (p4.IsMatching("/foo[bar]/"));
  EXPECT_FALSE (p4.IsMatching("/foorab"));
  EXPECT_FALSE (p4.IsMatching("/foobar"));

  EXPECT_TRUE  (p5.IsMatching("foo(bar)"));
  EXPECT_TRUE  (p5.IsMatching("foo(bar)/"));
  EXPECT_FALSE (p5.IsMatching("foobar"));
  EXPECT_FALSE (p5.IsMatching("foobar/"));

  EXPECT_TRUE  (p6.IsMatching("/foo/bar{baz}"));
  EXPECT_TRUE  (p6.IsMatching("/foo/bar{baz}/"));
  EXPECT_FALSE (p6.IsMatching("/foo/barbaz"));
  EXPECT_FALSE (p6.IsMatching("/foo/barb/"));

  EXPECT_TRUE  (p7.IsMatching("foo/^bar"));
  EXPECT_TRUE  (p7.IsMatching("foo/^bar/"));
  EXPECT_FALSE (p7.IsMatching("foo/bar"));

  EXPECT_TRUE  (p8.IsMatching("foo/bar$"));
  EXPECT_TRUE  (p8.IsMatching("foo/bar$/"));
  EXPECT_FALSE (p8.IsMatching("foo/bar"));

  EXPECT_TRUE  (p9.IsMatching("/moep+test/foo"));
  EXPECT_TRUE  (p9.IsMatching("/moep+test/foo/"));
  EXPECT_FALSE (p9.IsMatching("/moeppptest/foo"));
  EXPECT_FALSE (p9.IsMatching("/moeptest/foo"));

  EXPECT_TRUE  (p10.IsMatching("moep\\dtest/foo"));
  EXPECT_TRUE  (p10.IsMatching("moep\\dtest/foo/"));
  EXPECT_FALSE (p10.IsMatching("moep1test/foo"));

  EXPECT_TRUE  (p11.IsMatching("/moep+/foo?/bar*/t[e]st/hallo.welt"));
  EXPECT_TRUE  (p11.IsMatching("/moep+/foo?/bar*/t[e]st/hallo.welt/"));
  EXPECT_FALSE (p11.IsMatching("/moep/foo?/bar*/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moeppp/foo?/bar*/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moeppp/foo/bar*/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo/bar*/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/fo/bar*/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo?/bar/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo?/ba/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo?/barrr/t[e]st/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo?/bar*/test/hallo.welt"));
  EXPECT_FALSE (p11.IsMatching("/moep+/foo?/bar*/t[e]st/halloxwelt"));
  EXPECT_FALSE (p11.IsMatching("/moep/fo/barrr/test/hallo0welt/"));
}


TEST(T_Pathspec, MatchWithWildcard) {
  const Pathspec p1("/hallo/welt.*");
  const Pathspec p2("/fo*o/b*r");
  const Pathspec p3("/foo/bar.*");
  const Pathspec p4("bar/txt.*");
  const Pathspec p5("/*/*");
  const Pathspec p6("*.test");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());
  EXPECT_TRUE (p3.IsValid());
  EXPECT_TRUE (p4.IsValid());
  EXPECT_TRUE (p5.IsValid());
  EXPECT_TRUE (p6.IsValid());

  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.foo"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.fooo"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt."));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.test"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt./"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.moep/"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt.mo/ep"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt.mo/ep"));

  EXPECT_TRUE  (p2.IsMatching("/foo/br"));
  EXPECT_TRUE  (p2.IsMatching("/foo/br/"));
  EXPECT_TRUE  (p2.IsMatching("/fo00o/bar"));
  EXPECT_TRUE  (p2.IsMatching("/fo012o/baaar"));
  EXPECT_TRUE  (p2.IsMatching("/fo00o0o/b.r"));
  EXPECT_FALSE (p2.IsMatching("/foo/b"));
  EXPECT_FALSE (p2.IsMatching("/foo/bar/test"));
  EXPECT_FALSE (p2.IsMatching("foo/bar"));
  EXPECT_FALSE (p2.IsMatching("/baz/foo/bar"));

  EXPECT_TRUE  (p3.IsMatching("/foo/bar.txt"));
  EXPECT_TRUE  (p3.IsMatching("/foo/bar.jpg"));
  EXPECT_TRUE  (p3.IsMatching("/foo/bar.exe"));
  EXPECT_TRUE  (p3.IsMatching("/foo/bar.png"));
  EXPECT_TRUE  (p3.IsMatching("/foo/bar.*"));
  EXPECT_TRUE  (p3.IsMatching("/foo/bar.d/"));
  EXPECT_FALSE (p3.IsMatching("/foo/bar"));
  EXPECT_FALSE (p3.IsMatching("/foo/bar/"));
  EXPECT_FALSE (p3.IsMatching("foo/bar.txt"));

  EXPECT_TRUE  (p4.IsMatching("bar/txt.jpg"));
  EXPECT_TRUE  (p4.IsMatching("bar/txt.png"));
  EXPECT_TRUE  (p4.IsMatching("bar/txt."));
  EXPECT_FALSE (p4.IsMatching("/bar/txt.meop"));
  EXPECT_FALSE (p4.IsMatching("bar/txt"));

  EXPECT_TRUE  (p5.IsMatching("/hallo/welt"));
  EXPECT_TRUE  (p5.IsMatching("/hallo/welt/"));
  EXPECT_TRUE  (p5.IsMatching("/foo/bar"));
  EXPECT_TRUE  (p5.IsMatching("/foo/"));
  EXPECT_FALSE (p5.IsMatching("/foo/bar/baz"));
  EXPECT_FALSE (p5.IsMatching("foo/bar"));

  EXPECT_TRUE  (p6.IsMatching("hallo.test"));
  EXPECT_TRUE  (p6.IsMatching(".test"));
  EXPECT_TRUE  (p6.IsMatching("1.test"));
  EXPECT_TRUE  (p6.IsMatching("..test"));
  EXPECT_FALSE (p6.IsMatching("/hallo.test"));
  EXPECT_FALSE (p6.IsMatching("/.test"));
  EXPECT_FALSE (p6.IsMatching("/test"));
  EXPECT_FALSE (p6.IsMatching("test"));
}


TEST(T_Pathspec, MatchWithPlaceholders) {
  const Pathspec p1("/hallo/welt.???");
  const Pathspec p2("f?o/b?r");
  const Pathspec p3("/foo/?bar");

  EXPECT_TRUE (p1.IsValid());
  EXPECT_TRUE (p2.IsValid());
  EXPECT_TRUE (p3.IsValid());

  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.txt"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.jpg"));
  EXPECT_TRUE  (p1.IsMatching("/hallo/welt.dir/"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt.jpeg"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt.html"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt.so"));
  EXPECT_FALSE (p1.IsMatching("/hallo/welt."));

  EXPECT_TRUE  (p2.IsMatching("foo/bar"));
  EXPECT_TRUE  (p2.IsMatching("f0o/b4r"));
  EXPECT_TRUE  (p2.IsMatching("f?o/b?r"));
  EXPECT_TRUE  (p2.IsMatching("f+o/b+r"));
  EXPECT_TRUE  (p2.IsMatching("foo/bar/"));
  EXPECT_FALSE (p2.IsMatching("/foo/bar"));
  EXPECT_FALSE (p2.IsMatching("fo0oo/ba4ar"));
  EXPECT_FALSE (p2.IsMatching("fo/br"));
  EXPECT_FALSE (p2.IsMatching("fooo/br"));

  EXPECT_TRUE  (p3.IsMatching("/foo/1bar"));
  EXPECT_TRUE  (p3.IsMatching("/foo/2bar"));
  EXPECT_TRUE  (p3.IsMatching("/foo/?bar"));
  EXPECT_TRUE  (p3.IsMatching("/foo/?bar/"));
  EXPECT_TRUE  (p3.IsMatching("/foo/\\bar"));
  EXPECT_FALSE (p3.IsMatching("foo/1bar"));
  EXPECT_FALSE (p3.IsMatching("foo/bar"));
  EXPECT_FALSE (p3.IsMatching("/foo/bar"));
}


TEST(T_Pathspec, ComparePathspecs) {
  const Pathspec p1("/hallo/welt");
  const Pathspec p2("/hallo/welt");
  const Pathspec p3("/hallo/welt/");
  const Pathspec p4("/hallo/wel?");
  const Pathspec p5("/hallo/wel*");
  const Pathspec p6("/hallo/welt/moep");
  const Pathspec p7("/hallo/*/moep");
  const Pathspec p8("/hallo/*/moep");
  const Pathspec p9("/hallo/tlew");
  const Pathspec p10("hallo/welt");
  const Pathspec p11("hallo/welt");
  const Pathspec p12("hallo/*/welt");
  const Pathspec p13("hallo/*/welt");
  const Pathspec p14("hallo/?/welt");
  const Pathspec p15("hallo/?/welt");
  const Pathspec p16("ha??o/*/w??t");
  const Pathspec p17("ha??o/*/w??t");

  EXPECT_EQ (p1, p2);
  EXPECT_EQ (p2, p1);
  EXPECT_EQ (p2, p3);
  EXPECT_EQ (p3, p1);
  EXPECT_EQ (p3, p2);
  EXPECT_EQ (p7, p8);
  EXPECT_EQ (p8, p7);
  EXPECT_NE (p1, p4);
  EXPECT_NE (p4, p1);
  EXPECT_NE (p4, p5);
  EXPECT_NE (p5, p4);
  EXPECT_NE (p6, p7);
  EXPECT_NE (p7, p6);
  EXPECT_NE (p5, p6);
  EXPECT_NE (p6, p5);
  EXPECT_NE (p9, p1);
  EXPECT_NE (p1, p9);
  EXPECT_NE (p9, p3);
  EXPECT_NE (p3, p9);
  EXPECT_NE (p1, p10);
  EXPECT_NE (p3, p9);
  EXPECT_EQ (p10, p11);
  EXPECT_EQ (p11, p10);
  EXPECT_EQ (p12, p13);
  EXPECT_EQ (p13, p12);
  EXPECT_NE (p13, p14);
  EXPECT_NE (p14, p13);
  EXPECT_EQ (p14, p15);
  EXPECT_EQ (p15, p14);
  EXPECT_EQ (p16, p17);
  EXPECT_EQ (p17, p16);
  EXPECT_NE (p16, p14);
  EXPECT_NE (p14, p16);
  EXPECT_NE (p12, p16);
  EXPECT_NE (p16, p12);
}
