/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <string>

#include "path_filters/inclusion_spec.h"
#include "util/posix.h"

class T_InclusionSpec : public ::testing::Test {
 protected:
  catalog::InclusionSpec spec;
};


TEST_F(T_InclusionSpec, ValidVersionParsing) {
  EXPECT_TRUE(spec.Parse("version 1\n"
                         "/some/path\n"));
  EXPECT_TRUE(spec.IsValid());
  EXPECT_EQ(1, spec.version());
}


TEST_F(T_InclusionSpec, VersionWithLeadingComments) {
  EXPECT_TRUE(spec.Parse("# This is a comment\n"
                         "\n"
                         "version 1\n"
                         "/some/path\n"));
  EXPECT_TRUE(spec.IsValid());
  EXPECT_EQ(1, spec.version());
}


TEST_F(T_InclusionSpec, MissingVersion) {
  EXPECT_FALSE(spec.Parse("/some/path\n"));
  EXPECT_FALSE(spec.IsValid());
}


TEST_F(T_InclusionSpec, UnsupportedVersion) {
  EXPECT_FALSE(spec.Parse("version 99\n"
                          "/some/path\n"));
  EXPECT_FALSE(spec.IsValid());
  EXPECT_EQ(99, spec.version());
}


TEST_F(T_InclusionSpec, EmptySpec) {
  EXPECT_FALSE(spec.Parse(""));
  EXPECT_FALSE(spec.IsValid());
}


TEST_F(T_InclusionSpec, CommentsOnly) {
  EXPECT_FALSE(spec.Parse("# just a comment\n"
                          "# another comment\n"));
  EXPECT_FALSE(spec.IsValid());
}


TEST_F(T_InclusionSpec, VersionOnly) {
  EXPECT_TRUE(spec.Parse("version 1\n"));
  EXPECT_TRUE(spec.IsValid());
}


TEST_F(T_InclusionSpec, BasicInclusion) {
  EXPECT_TRUE(spec.Parse("version 1\n"
                         "/software/releases/old\n"
                         "/data/simulation/2019\n"));
  EXPECT_TRUE(spec.IsValid());

  // Root is never excluded
  EXPECT_FALSE(spec.IsExcluded(""));
  EXPECT_FALSE(spec.IsExcluded("/"));

  // Included paths are not excluded
  EXPECT_FALSE(spec.IsExcluded("/software/releases/old"));
  EXPECT_FALSE(spec.IsExcluded("/data/simulation/2019"));

  // Sub-paths of included paths are also included
  EXPECT_FALSE(spec.IsExcluded("/software/releases/old/sub1"));
  EXPECT_FALSE(spec.IsExcluded("/data/simulation/2019/run1"));

  // Parent paths of included paths are not excluded either: RelaxedPathFilter
  // matches them positively so their catalogs/objects stay replicable as the
  // path to the included subtree.
  EXPECT_FALSE(spec.IsExcluded("/software"));
  EXPECT_FALSE(spec.IsExcluded("/software/releases"));

  // Anything not covered by an inclusion rule is excluded
  EXPECT_TRUE(spec.IsExcluded("/software/releases/new"));
  EXPECT_TRUE(spec.IsExcluded("/data/simulation/2020"));
  EXPECT_TRUE(spec.IsExcluded("/completely/different"));
}


TEST_F(T_InclusionSpec, Negation) {
  EXPECT_TRUE(spec.Parse("version 1\n"
                         "/software/releases/old\n"
                         "!/software/releases/old/critical\n"));
  EXPECT_TRUE(spec.IsValid());

  // Included
  EXPECT_FALSE(spec.IsExcluded("/software/releases/old"));
  EXPECT_FALSE(spec.IsExcluded("/software/releases/old/other"));

  // Excluded via negation
  EXPECT_TRUE(spec.IsExcluded("/software/releases/old/critical"));
  EXPECT_TRUE(spec.IsExcluded("/software/releases/old/critical/sub"));
}


TEST_F(T_InclusionSpec, WildcardInclusion) {
  EXPECT_TRUE(spec.Parse("version 1\n"
                         "/data/scratch/*\n"));
  EXPECT_TRUE(spec.IsValid());

  EXPECT_FALSE(spec.IsExcluded("/data/scratch/temp1"));
  EXPECT_FALSE(spec.IsExcluded("/data/scratch/temp2"));
  // The parent path /data/scratch itself
  // With RelaxedPathFilter, parents of matching paths also match
  EXPECT_FALSE(spec.IsExcluded("/data/scratch"));

  // Unrelated paths are excluded
  EXPECT_TRUE(spec.IsExcluded("/data/other"));
}


TEST_F(T_InclusionSpec, TrailingSlashIgnored) {
  EXPECT_TRUE(spec.Parse("version 1\n"
                         "/software/releases/old/\n"));
  EXPECT_TRUE(spec.IsValid());

  EXPECT_FALSE(spec.IsExcluded("/software/releases/old"));
  EXPECT_FALSE(spec.IsExcluded("/software/releases/old/sub"));
  EXPECT_TRUE(spec.IsExcluded("/software/releases/new"));
}


TEST_F(T_InclusionSpec, InvalidPathNotExcluded) {
  // Without successful parse, nothing is excluded
  catalog::InclusionSpec bad_spec;
  EXPECT_FALSE(bad_spec.IsExcluded("/any/path"));
}


TEST_F(T_InclusionSpec, CreateFromFile) {
  std::string content = "version 1\n/sw/repo/ASG\n";
  std::string spec_path;
  FILE *f = CreateTempFile("./cvmfs-spec", 0600, "w", &spec_path);
  ASSERT_TRUE(f != NULL);
  fwrite(content.data(), content.size(), 1, f);
  fclose(f);

  catalog::InclusionSpec *spec_from_file =
      catalog::InclusionSpec::Create(spec_path);
  ASSERT_TRUE(spec_from_file != NULL);
  EXPECT_TRUE(spec_from_file->IsValid());
  EXPECT_EQ(1, spec_from_file->version());
  EXPECT_FALSE(spec_from_file->IsExcluded("/sw/repo/ASG"));
  EXPECT_FALSE(spec_from_file->IsExcluded("/sw/repo/ASG/AnalysisTop"));
  EXPECT_TRUE(spec_from_file->IsExcluded("/sw/repo/other"));

  delete spec_from_file;
  unlink(spec_path.c_str());
}


TEST_F(T_InclusionSpec, CreateFromNonexistentFile) {
  catalog::InclusionSpec *spec_from_file =
      catalog::InclusionSpec::Create("/nonexistent/path/to/spec");
  EXPECT_TRUE(spec_from_file == NULL);
}


TEST_F(T_InclusionSpec, ContentPreserved) {
  std::string input = "version 1\n/some/path\n";
  EXPECT_TRUE(spec.Parse(input));
  EXPECT_EQ(input, spec.content());
}
