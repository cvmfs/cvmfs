/**
 * This file is part of the CernVM File System.
 */
#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>


namespace fuzzy {

int GetDLDistance(const std::string &a, const std::string &b);
double GetYWDistance(const std::string &a, const std::string &b);

struct BKNode {
  std::string word;
  std::unordered_map<int, std::unique_ptr<BKNode> > children;

  explicit BKNode(const std::string &w) : word(w) { }
};

class BKTree {
  std::unique_ptr<BKNode> root;
  using DistanceFunc = int (*)(const std::string &, const std::string &);
  DistanceFunc distance;

  void add(BKNode *node, const std::string &word) {
    const int d = distance(node->word, word);
    if (node->children.find(d) == node->children.end()) {
      node->children[d] = std::unique_ptr<BKNode>(new BKNode{word});
    } else {
      add(node->children[d].get(), word);
    }
  }

  void query(const BKNode *node, const std::string &word, int threshold,
             std::vector<std::pair<std::string, int> > &results) const {
    const int d = distance(node->word, word);
    if (d <= threshold)
      results.emplace_back(node->word, d);

    const int start = std::max(1, d - threshold);
    const int end = d + threshold;
    for (const auto &[dist, child] : node->children) {
      if (dist >= start && dist <= end) {
        query(child.get(), word, threshold, results);
      }
    }
  }

 public:
  explicit BKTree(DistanceFunc dist) : distance(dist) { }

  void insert(const std::string &word) {
    if (!root) {
      root = std::unique_ptr<BKNode>(new BKNode(word));
    } else {
      add(root.get(), word);
    }
  }

  std::vector<std::pair<std::string, int> > search(const std::string &word,
                                                   int threshold) const {
    std::vector<std::pair<std::string, int> > results;
    if (root)
      query(root.get(), word, threshold, results);
    return results;
  }
};

class FuzzySearch {
  BKTree tree;
  int max_edits;

 public:
  explicit FuzzySearch(const std::vector<std::string> &dictionary,
                       int max_edits = 2)
      : tree(GetDLDistance), max_edits(max_edits) {
    for (const auto &word : dictionary) {
      tree.insert(word);
    }
  }

  std::string search(const std::string &query) const {
    auto candidates = tree.search(query, max_edits);
    if (candidates.empty())
      return "";

    // Replace std::ranges::sort with std::sort
    std::sort(candidates.begin(), candidates.end(),
              [&query](const auto &a, const auto &b) {
                return GetYWDistance(query, a.first)
                       > GetYWDistance(query, b.first);
              });

    return candidates[0].first;
  }
};

}  // namespace fuzzy

