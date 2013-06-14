
#include "catalog_sql.h"

namespace catalog {

template<typename FieldT>
typename TreeCountersBase<FieldT>::FieldsMap
    TreeCountersBase<FieldT>::GetFieldsMap() const {
  FieldsMap map;
  self.FillFieldsMap(map, "self_");
  subtree.FillFieldsMap(map, "subtree_");
  return map;
}


template<typename FieldT>
bool TreeCountersBase<FieldT>::ReadFromDatabase(const Database &database) {
  bool retval = true;

  FieldsMap map = GetFieldsMap();
  SqlGetCounter sql_counter(database);

  typename FieldsMap::const_iterator i    = map.begin();
  typename FieldsMap::const_iterator iend = map.end();
  for (; i != iend; ++i) {
    const bool current_retval = sql_counter.BindCounter(i->first) &&
                                sql_counter.Execute();
    *(const_cast<FieldT*>(i->second)) =
                                  static_cast<FieldT>(sql_counter.GetCounter());
    sql_counter.Reset();

    retval = (retval) ? current_retval : false;
  }

  return retval;
}


template<typename FieldT>
bool TreeCountersBase<FieldT>::WriteToDatabase(const Database &database) const {
  bool retval = true;

  const FieldsMap map = GetFieldsMap();
  SqlUpdateCounter sql_counter(database);

  typename FieldsMap::const_iterator i    = map.begin();
  typename FieldsMap::const_iterator iend = map.end();
  for (; i != iend; ++i) {
    const bool current_retval =
      sql_counter.BindCounter(i->first)   &&
      sql_counter.BindDelta(*(i->second)) &&
      sql_counter.Execute();
    sql_counter.Reset();

    retval = (retval) ? current_retval : false;
  }

  return retval;
}


template<typename FieldT>
bool TreeCountersBase<FieldT>::InsertIntoDatabase(const Database &database) const {
  bool retval = true;

  const FieldsMap map = GetFieldsMap();
  SqlCreateCounter sql_counter(database);

  typename FieldsMap::const_iterator i    = map.begin();
  typename FieldsMap::const_iterator iend = map.end();
  for (; i != iend; ++i) {
    const bool current_retval =
      sql_counter.BindCounter(i->first)   &&
      sql_counter.BindInitialValue(*(i->second)) &&
      sql_counter.Execute();
    sql_counter.Reset();

    retval = (retval) ? current_retval : false;
  }

  return retval;
}


template<typename FieldT>
void TreeCountersBase<FieldT>::SetZero() {
  self.Subtract(self);
  subtree.Subtract(subtree);
}


} /* namespace catalog */
