#include <gtest/gtest.h>
#include <string>

#include "../../cvmfs/util.h"

#include "testutil.h"

struct DecisionType {
  DecisionType() : type(-1), fail(false) {}
  int   type;
  bool  fail;
};

struct IntrospectionType {
  IntrospectionType(const int type, const std::string &message) :
    message(message), type(type) {}
  std::string  message;
  int          type;
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
    return !param_.fail;
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
 public:
  static const int          type_id = 0;
  static const std::string  message;

  static unsigned int constructor_calls;
  static void Reset() {
    FirstPolyCtorMock::constructor_calls = 0;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == FirstPolyCtorMock::type_id);
  }

  FirstPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    FirstPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(FirstPolyCtorMock::type_id,
                             FirstPolyCtorMock::message);
  }

};
unsigned int      FirstPolyCtorMock::constructor_calls = 0;
const std::string FirstPolyCtorMock::message           = "Hello from First.";

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class SecondPolyCtorMock : public AbstractPolyCtorMock {
 public:
  static const int          type_id = 1;
  static const std::string  message;

  static unsigned int constructor_calls;
  static void Reset() {
    SecondPolyCtorMock::constructor_calls = 0;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == SecondPolyCtorMock::type_id);
  }

  SecondPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    SecondPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(SecondPolyCtorMock::type_id,
                             SecondPolyCtorMock::message);
  }

};
unsigned int      SecondPolyCtorMock::constructor_calls = 0;
const std::string SecondPolyCtorMock::message           = "Second calling!";

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

class ThirdPolyCtorMock : public AbstractPolyCtorMock {
 public:
  static const int          type_id = 2;
  static const std::string  message;

  static unsigned int constructor_calls;
  static void Reset() {
    ThirdPolyCtorMock::constructor_calls = 0;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == ThirdPolyCtorMock::type_id);
  }

  ThirdPolyCtorMock(const DecisionType &param) : AbstractPolyCtorMock(param) {
    ThirdPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(ThirdPolyCtorMock::type_id,
                             ThirdPolyCtorMock::message);
  }

};
unsigned int      ThirdPolyCtorMock::constructor_calls = 0;
const std::string ThirdPolyCtorMock::message           = "Third à l'appareil.";

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//

void AbstractPolyCtorMock::RegisterPlugins() {
  AbstractPolyCtorMock::register_plugin_calls++;
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

typedef AbstractPolyCtorMock::IntrospectionData IntrospectionData;

//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


class T_PolymorphicConstruction : public ::testing::Test {
 protected:
  void SetUp() {
    PolymorphicConstructionUnittestAdapter::UnregisterAllPlugins<AbstractPolyCtorMock>();
    AbstractPolyCtorMock::ResetAll();
  }
};


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_PolymorphicConstruction, Noop) {
  EXPECT_EQ (0u, AbstractPolyCtorMock::register_plugin_calls);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_PolymorphicConstruction, Introspect) {
  EXPECT_EQ (0u, AbstractPolyCtorMock::register_plugin_calls);
  IntrospectionData v = AbstractPolyCtorMock::Introspect();
  EXPECT_EQ (1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ (0u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ (0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ (0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ (0u, ThirdPolyCtorMock::constructor_calls);

  EXPECT_EQ (3u, v.size());
  bool found_1 = false; bool found_2 = false; bool found_3 = false;
  EXPECT_TRUE (!found_1 && !found_2 && !found_3);
  IntrospectionData::const_iterator i    = v.begin();
  IntrospectionData::const_iterator iend = v.end();
  for (; i != iend; ++i) {
    if (i->type    == FirstPolyCtorMock::type_id &&
        i->message == FirstPolyCtorMock::message) {
      found_1 = true;
    }
    if (i->type    == SecondPolyCtorMock::type_id &&
        i->message == SecondPolyCtorMock::message) {
      found_2 = true;
    }
    if (i->type    == ThirdPolyCtorMock::type_id &&
        i->message == ThirdPolyCtorMock::message) {
      found_3 = true;
    }
  }
  EXPECT_TRUE (found_1 && found_2 && found_3);

  IntrospectionData w = AbstractPolyCtorMock::Introspect();
  EXPECT_EQ (1u, AbstractPolyCtorMock::register_plugin_calls);
}


//
// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
//


TEST_F(T_PolymorphicConstruction, CreateFirst) {
  EXPECT_EQ (0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 0;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_NE (static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ (1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ (1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ (1u, FirstPolyCtorMock::constructor_calls);
}
