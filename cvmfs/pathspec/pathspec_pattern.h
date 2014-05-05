/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_PATTERN_H_
#define CVMFS_PATHSPEC_PATTERN_H_

#include <string>
#include <vector>

class PathspecElementPattern {
 private:
  static const char kEscaper     = '\\';
  static const char kWildcard    = '*';
  static const char kPlaceholder = '?';

 private:
  class SubPattern {
   public:
    SubPattern() {}
    virtual ~SubPattern() {}
    virtual SubPattern* Clone() const = 0;
    virtual bool IsEmpty() const { return false; }
  };

  class PlaintextSubPattern : public SubPattern {
   public:
    PlaintextSubPattern() : SubPattern() {}
    PlaintextSubPattern(const PlaintextSubPattern &other) :
      chars_(other.chars_) {}
    SubPattern* Clone() const { return new PlaintextSubPattern(*this); }

    void AddChar(const char chr);
    bool IsEmpty() const { return chars_.empty(); }

   private:
    std::string chars_;
  };

  class WildcardSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new WildcardSubPattern(); }
  };

  class PlaceholderSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new PlaceholderSubPattern(); }
  };

 private:
  typedef std::vector<SubPattern*> SubPatterns;

 public:
  PathspecElementPattern(const std::string::const_iterator   begin,
                         const std::string::const_iterator  &end);
  PathspecElementPattern(const PathspecElementPattern& other);
  // TODO: C++11 - move constructor!
  ~PathspecElementPattern();

  bool Matches(const std::string::const_iterator   begin,
               const std::string::const_iterator  &end) const;
  bool Matches(const std::string &item) const {
    return Matches(item.begin(), item.end());
  }

 protected:
  void Parse(                  const std::string::const_iterator  &begin,
                               const std::string::const_iterator  &end);
  SubPattern* ParsePlaintext(        std::string::const_iterator  &i,
                               const std::string::const_iterator  &end);
  SubPattern* ParseSpecialChar(      std::string::const_iterator  &i,
                               const std::string::const_iterator  &end);

  bool IsSpecialChar(const char chr) const {
    return (chr == kWildcard || chr == kPlaceholder);
  }

 private:
  bool valid_;
  SubPatterns subpatterns_;
};

#endif  // CVMFS_PATHSPEC_PATTERN_H_
