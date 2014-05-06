/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_PATTERN_H_
#define CVMFS_PATHSPEC_PATTERN_H_

#include <string>
#include <vector>

class PathspecElementPattern {
 private:
  class SubPattern {
   public:
    SubPattern() {}
    virtual ~SubPattern() {}
    virtual SubPattern* Clone() const = 0;
    virtual bool IsEmpty() const { return false; }

    virtual std::string GenerateRegularExpression() const = 0;
  };

  class PlaintextSubPattern : public SubPattern {
   public:
    PlaintextSubPattern() : SubPattern() {}
    PlaintextSubPattern(const PlaintextSubPattern &other) :
      chars_(other.chars_) {}
    SubPattern* Clone() const { return new PlaintextSubPattern(*this); }

    void AddChar(const char chr);
    bool IsEmpty() const { return chars_.empty(); }

    std::string GenerateRegularExpression() const;

   protected:
    bool IsSpecialRegexCharacter(const char chr) const;

   private:
    std::string chars_;
  };

  class WildcardSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new WildcardSubPattern(); }
    std::string GenerateRegularExpression() const;
  };

  class PlaceholderSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new PlaceholderSubPattern(); }
    std::string GenerateRegularExpression() const;
  };

 private:
  typedef std::vector<SubPattern*> SubPatterns;

 public:
  PathspecElementPattern(const std::string::const_iterator   begin,
                         const std::string::const_iterator  &end);
  PathspecElementPattern(const PathspecElementPattern& other);
  // TODO: C++11 - move constructor!
  ~PathspecElementPattern();

  std::string GenerateRegularExpression() const;
  bool IsValid() const { return valid_; }

 protected:
  void Parse(                  const std::string::const_iterator  &begin,
                               const std::string::const_iterator  &end);
  SubPattern* ParsePlaintext(        std::string::const_iterator  &i,
                               const std::string::const_iterator  &end);
  SubPattern* ParseSpecialChar(      std::string::const_iterator  &i,
                               const std::string::const_iterator  &end);

 private:
  bool valid_;

  SubPatterns subpatterns_;
};

#endif  // CVMFS_PATHSPEC_PATTERN_H_
