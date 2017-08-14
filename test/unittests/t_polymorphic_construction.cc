/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include <cstdlib>
#include <string>

#include "prng.h"
#include "../common/testutil.h"
#include "util/plugin.h"

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

class AbstractPolyCtorMock : public
  PolymorphicConstruction<AbstractPolyCtorMock, DecisionType, IntrospectionType>
{
 public:
  static unsigned int constructor_calls;
  static unsigned int initialize_calls;
  static unsigned int initializes_failed;
  static unsigned int register_plugin_calls;
  static void Reset() {
    AbstractPolyCtorMock::constructor_calls     = 0;
    AbstractPolyCtorMock::initialize_calls      = 0;
    AbstractPolyCtorMock::initializes_failed    = 0;
    AbstractPolyCtorMock::register_plugin_calls = 0;
  }

  static void ResetAll();

 public:
  static void RegisterPlugins();

  explicit AbstractPolyCtorMock(const DecisionType &param) : param_(param) {
    AbstractPolyCtorMock::constructor_calls++;
  }

  virtual bool Initialize() {
    AbstractPolyCtorMock::initialize_calls++;
    if (param_.fail) {
      AbstractPolyCtorMock::initializes_failed++;
    }
    return !param_.fail;
  }

 protected:
  const DecisionType param_;
};
unsigned int AbstractPolyCtorMock::constructor_calls     = 0;
unsigned int AbstractPolyCtorMock::initialize_calls      = 0;
unsigned int AbstractPolyCtorMock::initializes_failed    = 0;
unsigned int AbstractPolyCtorMock::register_plugin_calls = 0;


//------------------------------------------------------------------------------


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

  explicit FirstPolyCtorMock(const DecisionType &param)
    : AbstractPolyCtorMock(param)
  {
    FirstPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(FirstPolyCtorMock::type_id,
                             FirstPolyCtorMock::message);
  }
};
unsigned int      FirstPolyCtorMock::constructor_calls = 0;
const std::string FirstPolyCtorMock::message           = "Hello from First.";


//------------------------------------------------------------------------------


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

  explicit SecondPolyCtorMock(const DecisionType &param)
    : AbstractPolyCtorMock(param)
  {
    SecondPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(SecondPolyCtorMock::type_id,
                             SecondPolyCtorMock::message);
  }
};
unsigned int      SecondPolyCtorMock::constructor_calls = 0;
const std::string SecondPolyCtorMock::message           = "Second calling!";


//------------------------------------------------------------------------------


class ThirdPolyCtorMock : public AbstractPolyCtorMock {
 public:
  static const int          type_id = 2;
  static const std::string  message;

  static unsigned int constructor_calls;
  static unsigned int initialize_calls;
  static void Reset() {
    ThirdPolyCtorMock::constructor_calls = 0;
    ThirdPolyCtorMock::initialize_calls  = 0;
  }

 public:
  static bool WillHandle(const DecisionType &param) {
    return (param.type == ThirdPolyCtorMock::type_id);
  }

  explicit ThirdPolyCtorMock(const DecisionType &param)
    : AbstractPolyCtorMock(param)
  {
    ThirdPolyCtorMock::constructor_calls++;
  }

  static IntrospectionType GetInfo() {
    return IntrospectionType(ThirdPolyCtorMock::type_id,
                             ThirdPolyCtorMock::message);
  }

  bool Initialize() {
    ThirdPolyCtorMock::initialize_calls++;
    return AbstractPolyCtorMock::Initialize();
  }
};
unsigned int      ThirdPolyCtorMock::constructor_calls = 0;
unsigned int      ThirdPolyCtorMock::initialize_calls  = 0;
const std::string ThirdPolyCtorMock::message           = "Third à l'appareil.";


//------------------------------------------------------------------------------


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


//------------------------------------------------------------------------------


class T_PolymorphicConstruction : public ::testing::Test {
 protected:
  void SetUp() {
    PolymorphicConstructionUnittestAdapter::
      UnregisterAllPlugins<AbstractPolyCtorMock>();
    AbstractPolyCtorMock::ResetAll();
  }
};


//------------------------------------------------------------------------------


TEST_F(T_PolymorphicConstruction, Noop) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);
}


TEST_F(T_PolymorphicConstruction, Introspect) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);
  IntrospectionData v = AbstractPolyCtorMock::Introspect();
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(0u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, ThirdPolyCtorMock::constructor_calls);

  EXPECT_EQ(3u, v.size());
  bool found_1 = false; bool found_2 = false; bool found_3 = false;
  EXPECT_TRUE(!found_1 && !found_2 && !found_3);
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
  EXPECT_TRUE(found_1 && found_2 && found_3);

  IntrospectionData w = AbstractPolyCtorMock::Introspect();
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
}


TEST_F(T_PolymorphicConstruction, CreateFirst) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 0;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(1u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, ThirdPolyCtorMock::constructor_calls);

  delete mock;
}


TEST_F(T_PolymorphicConstruction, CreateSecond) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 1;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, ThirdPolyCtorMock::constructor_calls);

  delete mock;
}


TEST_F(T_PolymorphicConstruction, CreateThird) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 2;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::initialize_calls);

  delete mock;
}


TEST_F(T_PolymorphicConstruction, CreateUnknown) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = -1;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_EQ(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(0u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, ThirdPolyCtorMock::constructor_calls);
}


TEST_F(T_PolymorphicConstruction, CreateAll) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  for (int i = 0; i < 3; ++i) {
    DecisionType t;
    t.type = i;
    AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
    EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
    delete mock;
  }

  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(3u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(3u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(1u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::initialize_calls);
}


TEST_F(T_PolymorphicConstruction, CreateMany) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  const unsigned int runs = 100000;
  unsigned int ctors[] = { 0, 0, 0, 0 };

  Prng prng;
  prng.InitLocaltime();
  for (unsigned int i = 0; i < runs; ++i) {
    DecisionType t;
    t.type = prng.Next(4);
    AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
    if (t.type == 3) {
      EXPECT_EQ(static_cast<AbstractPolyCtorMock*>(NULL), mock);
    } else {
      EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
      delete mock;
    }
    ++ctors[t.type];
  }

  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);

  const unsigned int all_ctors = ctors[0] + ctors[1] + ctors[2];
  const unsigned int fails     = runs - all_ctors;
  EXPECT_EQ(all_ctors, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(all_ctors, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(ctors[0],  FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[1],  SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[2],  ThirdPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[2],  ThirdPolyCtorMock::initialize_calls);
  EXPECT_EQ(ctors[3],  fails);
}


TEST_F(T_PolymorphicConstruction, FailedInitOfFirst) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 0;
  t.fail = true;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_EQ(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(1u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, ThirdPolyCtorMock::constructor_calls);
}


TEST_F(T_PolymorphicConstruction, FailedInitOfThird) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  DecisionType t;
  t.type = 2;
  t.fail = true;
  AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
  EXPECT_EQ(static_cast<AbstractPolyCtorMock*>(NULL), mock);
  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(0u, FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(0u, SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::constructor_calls);
  EXPECT_EQ(1u, ThirdPolyCtorMock::initialize_calls);
}


TEST_F(T_PolymorphicConstruction, CreateManyWithFailures) {
  EXPECT_EQ(0u, AbstractPolyCtorMock::register_plugin_calls);

  const unsigned int runs = 100000;
  unsigned int ctors[] = { 0, 0, 0, 0 };
  unsigned int fails   = 0;

  Prng prng;
  prng.InitLocaltime();
  for (unsigned int i = 0; i < runs; ++i) {
    DecisionType t;
    t.type = prng.Next(4);
    t.fail = (prng.Next(2) == 0);
    AbstractPolyCtorMock *mock = AbstractPolyCtorMock::Construct(t);
    if (t.fail && t.type < 3) {
      ++fails;
    }
    if (t.type == 3 || t.fail) {
      EXPECT_EQ(static_cast<AbstractPolyCtorMock*>(NULL), mock);
    } else {
      EXPECT_NE(static_cast<AbstractPolyCtorMock*>(NULL), mock);
      delete mock;
    }
    ++ctors[t.type];
  }

  EXPECT_EQ(1u, AbstractPolyCtorMock::register_plugin_calls);

  const unsigned int all_ctors = ctors[0] + ctors[1] + ctors[2];
  const unsigned int unknowns  = runs - all_ctors;
  EXPECT_EQ(all_ctors, AbstractPolyCtorMock::constructor_calls);
  EXPECT_EQ(all_ctors, AbstractPolyCtorMock::initialize_calls);
  EXPECT_EQ(ctors[0],  FirstPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[1],  SecondPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[2],  ThirdPolyCtorMock::constructor_calls);
  EXPECT_EQ(ctors[2],  ThirdPolyCtorMock::initialize_calls);
  EXPECT_EQ(ctors[3],  unknowns);
  EXPECT_EQ(fails,     AbstractPolyCtorMock::initializes_failed);
}
