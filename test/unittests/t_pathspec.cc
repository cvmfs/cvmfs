#include <gtest/gtest.h>
#include <string>

#include "../../cvmfs/pathspec/pathspec.h"


TEST(T_Pathspec, ParseSimpleRelative) {
  const Pathspec p1("foo");
  const Pathspec p2("foo/bar");
  const Pathspec p3("foo/bar.baz");
  const Pathspec p4("foo/bar/baz.foo");
  const Pathspec p5(" foo");
  const Pathspec p6("  bar/baz/foo.bar");

  EXPECT_TRUE (p1.IsValid()); EXPECT_FALSE (p1.IsAbsolute()); EXPECT_FALSE (p1.IsNegation());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute()); EXPECT_FALSE (p2.IsNegation());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute()); EXPECT_FALSE (p3.IsNegation());
  EXPECT_TRUE (p4.IsValid()); EXPECT_FALSE (p4.IsAbsolute()); EXPECT_FALSE (p4.IsNegation());
  EXPECT_TRUE (p5.IsValid()); EXPECT_FALSE (p5.IsAbsolute()); EXPECT_FALSE (p5.IsNegation());
  EXPECT_TRUE (p6.IsValid()); EXPECT_FALSE (p6.IsAbsolute()); EXPECT_FALSE (p6.IsNegation());
}


TEST(T_Pathspec, ParseSimpleAbsolute) {
  const Pathspec p1("/");
  const Pathspec p2("/foo");
  const Pathspec p3("/foo/bar");
  const Pathspec p4("/foo/bar.baz");
  const Pathspec p5("/foo/bar/baz.foo");
  const Pathspec p6(" /foo/bar/baz.foo");
  const Pathspec p7("  /foo/bar/baz.foo");

  EXPECT_FALSE (p1.IsValid()); EXPECT_TRUE (p1.IsAbsolute()); EXPECT_FALSE (p1.IsNegation());
  EXPECT_TRUE  (p2.IsValid()); EXPECT_TRUE (p2.IsAbsolute()); EXPECT_FALSE (p2.IsNegation());
  EXPECT_TRUE  (p3.IsValid()); EXPECT_TRUE (p3.IsAbsolute()); EXPECT_FALSE (p3.IsNegation());
  EXPECT_TRUE  (p4.IsValid()); EXPECT_TRUE (p4.IsAbsolute()); EXPECT_FALSE (p4.IsNegation());
  EXPECT_TRUE  (p5.IsValid()); EXPECT_TRUE (p5.IsAbsolute()); EXPECT_FALSE (p5.IsNegation());
  EXPECT_TRUE  (p6.IsValid()); EXPECT_TRUE (p6.IsAbsolute()); EXPECT_FALSE (p6.IsNegation());
  EXPECT_TRUE  (p7.IsValid()); EXPECT_TRUE (p7.IsAbsolute()); EXPECT_FALSE (p7.IsNegation());
}


TEST(T_Pathspec, ParseSimpleNegation) {
  const Pathspec p1("!/");
  const Pathspec p2("! /foo");
  const Pathspec p3("!   foo/bar");
  const Pathspec p4("!foo/bar.baz");
  const Pathspec p5("! foo/bar/baz.foo");
  const Pathspec p6(" ! /foo/bar/baz.foo");
  const Pathspec p7("  ! foo/bar/baz.foo");

  EXPECT_FALSE (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute()); EXPECT_TRUE (p1.IsNegation());
  EXPECT_TRUE  (p2.IsValid()); EXPECT_TRUE  (p2.IsAbsolute()); EXPECT_TRUE (p2.IsNegation());
  EXPECT_TRUE  (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute()); EXPECT_TRUE (p3.IsNegation());
  EXPECT_TRUE  (p4.IsValid()); EXPECT_FALSE (p4.IsAbsolute()); EXPECT_TRUE (p4.IsNegation());
  EXPECT_TRUE  (p5.IsValid()); EXPECT_FALSE (p5.IsAbsolute()); EXPECT_TRUE (p5.IsNegation());
  EXPECT_TRUE  (p6.IsValid()); EXPECT_TRUE  (p6.IsAbsolute()); EXPECT_TRUE (p6.IsNegation());
  EXPECT_TRUE  (p7.IsValid()); EXPECT_FALSE (p7.IsAbsolute()); EXPECT_TRUE (p7.IsNegation());
}


TEST(T_Pathspec, ParsePlaceholders) {
  const Pathspec p1("!/hallo/??test/test");
  const Pathspec p2("foo?bar");
  const Pathspec p3("!bar.???");
  const Pathspec p4("/foo/bar/ba?");
  const Pathspec p5("/fo?/b?r/?az");

  EXPECT_TRUE (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute()); EXPECT_TRUE  (p1.IsNegation());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute()); EXPECT_FALSE (p2.IsNegation());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute()); EXPECT_TRUE  (p3.IsNegation());
  EXPECT_TRUE (p4.IsValid()); EXPECT_TRUE  (p4.IsAbsolute()); EXPECT_FALSE (p4.IsNegation());
  EXPECT_TRUE (p5.IsValid()); EXPECT_TRUE  (p5.IsAbsolute()); EXPECT_FALSE (p5.IsNegation());
}


TEST(T_Pathspec, ParseWildcards) {
  const Pathspec p1("!/hallo/*/test");
  const Pathspec p2("foo/*bar");
  const Pathspec p3("!bar.*");
  const Pathspec p4("/foo/b*r");
  const Pathspec p5("/foo/*/bar");

  EXPECT_TRUE (p1.IsValid()); EXPECT_TRUE  (p1.IsAbsolute()); EXPECT_TRUE  (p1.IsNegation());
  EXPECT_TRUE (p2.IsValid()); EXPECT_FALSE (p2.IsAbsolute()); EXPECT_FALSE (p2.IsNegation());
  EXPECT_TRUE (p3.IsValid()); EXPECT_FALSE (p3.IsAbsolute()); EXPECT_TRUE  (p3.IsNegation());
  EXPECT_TRUE (p4.IsValid()); EXPECT_TRUE  (p4.IsAbsolute()); EXPECT_FALSE (p4.IsNegation());
  EXPECT_TRUE (p5.IsValid()); EXPECT_TRUE  (p5.IsAbsolute()); EXPECT_FALSE (p5.IsNegation());
}
