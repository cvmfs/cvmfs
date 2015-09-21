/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_PATHSPEC_PATHSPEC_PATTERN_H_
#define CVMFS_PATHSPEC_PATHSPEC_PATTERN_H_

#include <string>
#include <vector>

/**
 * The PathspecElementPattern is used internally by the Pathspec class!
 *
 * It describes a part of a full Pathspec. Namely only one directory level.
 * Each PathspecElementPattern is composed by potentially multiple SubPatterns.
 *
 *  +----------+          +------------------------+          +------------+
 *  | Pathspec |----<>----| PathspecElementPattern |----<>----| SubPattern |
 *  +----------+          +------------------------+          +------------+
 +                                                                ^  ^  ^
 *                                                                |  |  |
 *             +--------------------------------------------------+  |  |
 *             |                        +----------------------------+  |
 *             |                        |                               |
 *  +---------------------+  +--------------------+  +-----------------------+
 *  | PlaintextSubPattern |  | WildcardSubPattern |  | PlaceholderSubPattern |
 *  +---------------------+  +--------------------+  +-----------------------+
 *
 * SubPatterns are implemented as a flat class hierarchy where the different
 * patterns are implementing the behaviour of one pattern symbol.
 *
 * The PathspecElementPattern is taking care of the parsing and creation of
 * SubPatterns.
 *
 */
class PathspecElementPattern {
 private:
  class SubPattern {
   public:
    SubPattern() {}
    virtual ~SubPattern() {}
    virtual SubPattern* Clone() const = 0;
    virtual bool IsEmpty() const { return false; }

    virtual bool Compare(const SubPattern *other) const = 0;

    virtual bool IsPlaintext()   const { return false; }
    virtual bool IsWildcard()    const { return false; }
    virtual bool IsPlaceholder() const { return false; }

    virtual std::string
      GenerateRegularExpression(const bool is_relaxed) const = 0;
    virtual std::string GenerateGlobString() const = 0;
  };

  class PlaintextSubPattern : public SubPattern {
   public:
    PlaintextSubPattern() : SubPattern() {}
    SubPattern* Clone() const { return new PlaintextSubPattern(*this); }
    bool Compare(const SubPattern *other) const;

    void AddChar(const char chr);
    bool IsEmpty() const { return chars_.empty(); }
    bool IsPlaintext() const { return true; }

    std::string GenerateRegularExpression(const bool is_relaxed) const;
    std::string GenerateGlobString()                             const;

   protected:
    PlaintextSubPattern(const PlaintextSubPattern &other) :
      chars_(other.chars_) {}
    PlaintextSubPattern& operator=(const PlaintextSubPattern &other);
    bool IsSpecialRegexCharacter(const char chr) const;

   private:
    std::string chars_;
  };

  class WildcardSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new WildcardSubPattern(); }
    bool Compare(const SubPattern *other) const;
    std::string GenerateRegularExpression(const bool is_relaxed) const;
    std::string GenerateGlobString()                             const;
    bool IsWildcard() const { return true; }
  };

  class PlaceholderSubPattern : public SubPattern {
   public:
    SubPattern* Clone() const { return new PlaceholderSubPattern(); }
    bool Compare(const SubPattern *other) const;
    std::string GenerateRegularExpression(const bool is_relaxed) const;
    std::string GenerateGlobString()                             const;
    bool IsPlaceholder() const { return true; }
  };

 private:
  typedef std::vector<SubPattern*> SubPatterns;

 public:
  PathspecElementPattern(const std::string::const_iterator   begin,
                         const std::string::const_iterator  &end);
  PathspecElementPattern(const PathspecElementPattern& other);
  PathspecElementPattern& operator=(const PathspecElementPattern& other);
  // TODO(rmeusel): C++11 - move constructor!
  ~PathspecElementPattern();

  std::string GenerateRegularExpression(const bool is_relaxed = false) const;
  std::string GenerateGlobString()                                     const;

  bool IsValid() const { return valid_; }

  bool operator== (const PathspecElementPattern &other) const;
  bool operator!= (const PathspecElementPattern &other) const {
    return !(*this == other);
  }

 protected:
  void Parse(const std::string::const_iterator &begin,
             const std::string::const_iterator &end);
  SubPattern* ParsePlaintext(const std::string::const_iterator &end,
                             std::string::const_iterator *i);
  SubPattern* ParseSpecialChar(const std::string::const_iterator &end,
                               std::string::const_iterator *i);

 private:
  bool valid_;
  SubPatterns subpatterns_;
};

#endif  // CVMFS_PATHSPEC_PATHSPEC_PATTERN_H_
