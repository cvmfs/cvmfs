/**
 * This file is part of the CernVM File System.
 */

namespace swissknife {

template <class DerivedT>
int Command<DerivedT>::Main(int argc, char **argv) {
  const ArgumentList arguments = ReadArguments(argc, argv);
  return Run(arguments);
}

template <class DerivedT>
ArgumentList Command<DerivedT>::ReadArguments(int argc, char **argv) const {
  ParameterList params = DerivedT::GetParameters();
  return ParseArguments(argc, argv, params);
}

} // namespace swissknife
