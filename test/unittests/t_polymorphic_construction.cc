#include <gtest/gtest.h>

#include "../../cvmfs/util.h"

struct DecisionType {
  DecisionType() : type(-1), fail(false) {}
  int   type;
  bool  fail;
};

struct IntrospectionType {
};

class AbstractPolyCtorMock : public PolymorphicConstruction<AbstractPolyCtorMock,
                                                            DecisionType,
                                                            IntrospectionType> {
 public:
  static unsigned int constructor_calls;
  static unsigned int initialize_calls;
  static unsigned int register_plugin_calls;
  static void Reset() {
    AbstractPolyCtorMock::constructor_calls     = 0;
    AbstractPolyCtorMock::initialize_calls      = 0;
    AbstractPolyCtorMock::register_plugin_calls = 0;
  }

  static void ResetAll();

 public:
  static void RegisterPlugins();

  AbstractPolyCtorMock(const DecisionType &param) : param_(param) {
    AbstractPolyCtorMock::constructor_calls++;
  }

  virtual bool Initialize() {
    AbstractPolyCtorMock::initialize_calls++;
    return param_.fail;
  }

 protected:
  const DecisionType param_;
};
unsigned int AbstractPolyCtorMock::constructor_calls     = 0;
unsigned int AbstractPolyCtorMock::initialize_calls      = 0;
unsigned int AbstractPolyCtorMock::register_plugin_calls = 0;

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class FirstPolyCtorMock : public AbstractPolyCtorMock {
 private:
  static const int type_id = 0;

 public:
  static unsigned int constructor_calls;
  static void Reset() {
    FirstPolyCtorMock::constructor_calls++;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == FirstPolyCtorMock::type_id);
  }

  FirstPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    FirstPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() { return IntrospectionType(); }

};
unsigned int FirstPolyCtorMock::constructor_calls = 0;

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class SecondPolyCtorMock : public AbstractPolyCtorMock {
 private:
  static const int type_id = 1;

 public:
  static unsigned int constructor_calls;
  static void Reset() {
    SecondPolyCtorMock::constructor_calls++;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == SecondPolyCtorMock::type_id);
  }

  SecondPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    SecondPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() { return IntrospectionType(); }

};
unsigned int SecondPolyCtorMock::constructor_calls = 0;

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class ThirdPolyCtorMock : public AbstractPolyCtorMock {
 private:
  static const int type_id = 2;

 public:
  static unsigned int constructor_calls;
  static void Reset() {
    ThirdPolyCtorMock::constructor_calls++;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == ThirdPolyCtorMock::type_id);
  }

  ThirdPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    ThirdPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() { return IntrospectionType(); }

};
unsigned int ThirdPolyCtorMock::constructor_calls = 0;

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

void AbstractPolyCtorMock::RegisterPlugins() {
  RegisterPlugin<FirstPolyCtorMock>();
  RegisterPlugin<SecondPolyCtorMock>();
  RegisterPlugin<ThirdPolyCtorMock>();
}

void AbstractPolyCtorMock::ResetAll() {
  AbstractPolyCtorMock::Reset();
  FirstPolyCtorMock::Reset();
  SecondPolyCtorMock::Reset();
  ThirdPolyCtorMock::Reset();
}

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class T_PolymorphicConstruction : public ::testing::Test {
 protected:
  void SetUp() {
    AbstractPolyCtorMock::Reset();
  }
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST(T_PolymorphicConstruction, Initialize) {

}
