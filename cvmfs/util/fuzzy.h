/**
 * This file is part of the CernVM File System.
 */
#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#ifdef CVMFS_UTIL_EXPORTS
#define CVMFS_UTIL_API __attribute__((visibility("default")))
#else
#define CVMFS_UTIL_API
#endif

namespace fuzzy {

CVMFS_UTIL_API int GetDLDistance(const std::string &a, const std::string &b);
CVMFS_UTIL_API double GetYWDistance(const std::string &a, const std::string &b);

struct BKNode {
  std::string word;
  std::unordered_map<int, std::unique_ptr<BKNode> > children;

  explicit BKNode(const std::string &w) : word(w) { }
};

class BKTree {
  std::unique_ptr<BKNode> root;
  using DistanceFunc = int (*)(const std::string &, const std::string &);
  DistanceFunc distance;

  void Add(BKNode *node, const std::string &word) {
    const int d = distance(node->word, word);
    if (node->children.find(d) == node->children.end()) {
      node->children[d] = std::unique_ptr<BKNode>(new BKNode{word});
    } else {
      Add(node->children[d].get(), word);
    }
  }

  void Query(const BKNode *node, const std::string &word, int threshold,
             std::vector<std::pair<std::string, int> > &results) const {
    const int d = distance(node->word, word);
    if (d <= threshold)
      results.emplace_back(node->word, d);

    const int start = std::max(1, d - threshold);
    const int end = d + threshold;
    for (const auto &[dist, child] : node->children) {
      if (dist >= start && dist <= end) {
        Query(child.get(), word, threshold, results);
      }
    }
  }

 public:
  explicit BKTree(DistanceFunc dist) : distance(dist) { }

  void Insert(const std::string &word) {
    if (!root) {
      root = std::unique_ptr<BKNode>(new BKNode(word));
    } else {
      Add(root.get(), word);
    }
  }

  std::vector<std::pair<std::string, int> > Search(const std::string &word,
                                                   int threshold) const {
    std::vector<std::pair<std::string, int> > results;
    if (root)
      Query(root.get(), word, threshold, results);
    return results;
  }
};

class FuzzySearch {
 public:
  explicit FuzzySearch(const std::vector<std::string> &dictionary,
                       int max_edits = 3)
      : tree(GetDLDistance), max_edits(max_edits) {
    for (const auto &word : dictionary) {
      tree.Insert(word);
    }
  }

  std::string Search(const std::string &Query) const {
    auto candidates = tree.Search(Query, max_edits);
    if (candidates.empty())
      return "";

    std::sort(candidates.begin(), candidates.end(), CandidateComparator(Query));

    return candidates[0].first;
  }

 private:
  static bool SortingCriterion(const std::string &Query,
                               const std::pair<std::string, int> &a,
                               const std::pair<std::string, int> &b) {
    const bool a_sub = a.first.find(Query) != std::string::npos;
    const bool b_sub = b.first.find(Query) != std::string::npos;
    if (a_sub != b_sub)
      return a_sub;
    return GetYWDistance(Query, a.first) > GetYWDistance(Query, b.first);
  }

  struct CandidateComparator {
    const std::string &Query;
    CandidateComparator(const std::string &q) : Query(q) { }
    bool operator()(const std::pair<std::string, int> &a,
                    const std::pair<std::string, int> &b) const {
      return FuzzySearch::SortingCriterion(Query, a, b);
    }
  };

  BKTree tree;
  int max_edits;
};

}  // namespace fuzzy
