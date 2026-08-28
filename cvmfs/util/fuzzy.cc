/**
 * This file is part of the CernVM File System.
 */
#include "fuzzy.h"
#include <vector>
#include <string>
#include <algorithm>

namespace fuzzy {
// Damerau-Levenshtein distance
int GetDLDistance(const std::string &a, const std::string &b) {
  const int m = static_cast<int>(a.size());
  const int n = static_cast<int>(b.size());
  std::vector<std::vector<int> > dp(m + 1, std::vector<int>(n + 1));

  for (int i = 0; i <= m; ++i)
    dp[i][0] = i;
  for (int j = 0; j <= n; ++j)
    dp[0][j] = j;

  for (int i = 1; i <= m; ++i) {
    for (int j = 1; j <= n; ++j) {
      const int cost = (a[i - 1] == b[j - 1]) ? 0 : 1;
      dp[i][j] = std::min({
          dp[i - 1][j] + 1,        // deletion
          dp[i][j - 1] + 1,        // insertion
          dp[i - 1][j - 1] + cost  // substitution
      });
      if (i > 1 && j > 1 && a[i - 1] == b[j - 2] && a[i - 2] == b[j - 1]) {
        dp[i][j] = std::min(dp[i][j],
                            dp[i - 2][j - 2] + cost);  // transposition
      }
    }
  }
  return dp[m][n];
}

// Jaro-Winkler similarity (0-1, higher is better)
double GetYWDistance(const std::string &a, const std::string &b) {
  if (a == b)
    return 1.0;
  if (a.empty() || b.empty())
    return 0.0;

  const int lenA = static_cast<int>(a.size());
  const int lenB = static_cast<int>(b.size());
  const int matchDistance = std::max(lenA, lenB) / 2 - 1;

  std::vector<bool> aMatches(lenA, false);
  std::vector<bool> bMatches(lenB, false);
  int matches = 0;

  for (int i = 0; i < lenA; ++i) {
    const int start = std::max(0, i - matchDistance);
    const int end = std::min(i + matchDistance + 1, lenB);
    for (int j = start; j < end; ++j) {
      if (!bMatches[j] && a[i] == b[j]) {
        aMatches[i] = bMatches[j] = true;
        ++matches;
        break;
      }
    }
  }
  if (matches == 0)
    return 0.0;

  int transpositions = 0;
  int k = 0;
  for (int i = 0; i < lenA; ++i) {
    if (aMatches[i]) {
      while (!bMatches[k])
        ++k;
      if (a[i] != b[k])
        ++transpositions;
      ++k;
    }
  }

  const double jaro = ((static_cast<double>(matches) / lenA)
                       + (static_cast<double>(matches) / lenB)
                       + (static_cast<double>(matches - transpositions / 2.0)
                          / matches))
                      / 3.0;

  int prefix = 0;
  for (int i = 0; i < std::min(4, std::min(lenA, lenB)); ++i) {
    if (a[i] == b[i])
      ++prefix;
    else
      break;
  }
  return jaro + 0.1 * prefix * (1 - jaro);
}
}  // namespace fuzzy

