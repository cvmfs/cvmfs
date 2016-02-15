/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "../../cvmfs/util.h"


class Foo {
 public:
  explicit Foo(const int identifier) :
    identifier(identifier), local_constructor_calls(0),
    local_destructor_calls(0), local_method_calls(0)
  {
    ++local_constructor_calls;
    ++Foo::global_constructor_calls;
  }

  ~Foo() {
    ++local_destructor_calls;
    ++Foo::global_destructor_calls;
  }

  int GetIdentifier() const {
    ++local_method_calls;
    ++Foo::global_method_calls;
    return identifier;
  }

  const int identifier;

          unsigned int  local_constructor_calls;
          unsigned int  local_destructor_calls;
  mutable unsigned int  local_method_calls;

  static unsigned int   global_constructor_calls;
  static unsigned int   global_destructor_calls;
  static unsigned int   global_method_calls;
};

unsigned int Foo::global_constructor_calls = 0;
unsigned int Foo::global_destructor_calls  = 0;
unsigned int Foo::global_method_calls      = 0;


class T_UniquePtr : public ::testing::Test {
 protected:
  void SetUp() {
    Foo::global_constructor_calls = 0;
    Foo::global_destructor_calls  = 0;
    Foo::global_method_calls      = 0;
  }
};


TEST_F(T_UniquePtr, NullInitialisation) {
  {
    UniquePtr<Foo> foo;
    EXPECT_FALSE(foo);
    EXPECT_FALSE(foo.IsValid());
    EXPECT_EQ(0u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    Foo *object = new Foo(42);
    EXPECT_EQ(1u, object->local_constructor_calls);

    foo = object;
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, DirectInitialisation) {
  {
    UniquePtr<Foo> foo(new Foo(1337));
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, WeakReference) {
  {
    UniquePtr<Foo> foo;
    EXPECT_FALSE(foo);
    EXPECT_FALSE(foo.IsValid());
    EXPECT_EQ(0u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    Foo *object = new Foo(27);
    foo = object;
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    EXPECT_EQ(object, foo.weak_ref());
    Foo *weak_foo = foo.weak_ref();
    EXPECT_EQ(27, weak_foo->GetIdentifier());
    EXPECT_EQ(1u, weak_foo->local_constructor_calls);
    EXPECT_EQ(1u, Foo::global_constructor_calls);
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, PointerDereference) {
  {
    Foo *object = new Foo(911);
    EXPECT_EQ(1u, object->local_constructor_calls);

    UniquePtr<Foo> foo(object);
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    EXPECT_EQ(911, (*foo).identifier);
    EXPECT_EQ(object, &(*foo));
    EXPECT_EQ(911, foo->identifier);

    const Foo& foo_reference = *foo;
    EXPECT_EQ(1u, foo->local_constructor_calls);
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, foo->local_destructor_calls);
    EXPECT_EQ(911, foo_reference.identifier);
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, PointerDereferenceAndMethodCall) {
  {
    Foo *object = new Foo(87465);
    EXPECT_EQ(1u, object->local_constructor_calls);

    UniquePtr<Foo> foo(object);
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    EXPECT_EQ(87465, foo->GetIdentifier());
    EXPECT_EQ(87465, (*foo).GetIdentifier());

    const Foo& foo_reference = *foo;
    EXPECT_EQ(1u, foo->local_constructor_calls);
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, foo->local_destructor_calls);
    EXPECT_EQ(87465, foo_reference.GetIdentifier());
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, ReleaseOwnership) {
  Foo *weak_foo = NULL;
  {
    Foo *object = new Foo(837456);
    EXPECT_EQ(1u, object->local_constructor_calls);

    UniquePtr<Foo> foo(object);
    EXPECT_TRUE(foo);
    EXPECT_TRUE(foo.IsValid());
    EXPECT_EQ(1u, Foo::global_constructor_calls);
    EXPECT_EQ(0u, Foo::global_destructor_calls);

    weak_foo = foo.Release();
    EXPECT_EQ(object, weak_foo);
    EXPECT_FALSE(foo);
    EXPECT_FALSE(foo.IsValid());
  }

  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  EXPECT_EQ(0u, weak_foo->local_destructor_calls);
  EXPECT_EQ(837456, weak_foo->GetIdentifier());

  delete weak_foo;
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}


TEST_F(T_UniquePtr, AssignmentOperator) {
  UniquePtr<Foo> foo(new Foo(12342));
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(1u, Foo::global_constructor_calls);

  EXPECT_EQ(0u, foo->local_destructor_calls);
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  foo = NULL;
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);

  foo = new Foo(871256);
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(2u, Foo::global_constructor_calls);

  EXPECT_EQ(0u, foo->local_destructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);

  EXPECT_EQ(871256, foo->GetIdentifier());
  EXPECT_EQ(1u, foo->local_method_calls);

  foo = new Foo(1343);
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(3u, Foo::global_constructor_calls);

  EXPECT_EQ(0u, foo->local_destructor_calls);
  EXPECT_EQ(2u, Foo::global_destructor_calls);

  EXPECT_EQ(1343, foo->GetIdentifier());
  EXPECT_EQ(1u, foo->local_method_calls);
}


TEST_F(T_UniquePtr, SelfAssignment) {
  Foo *bare_foo = new Foo(12342);
  UniquePtr<Foo> foo(bare_foo);
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  foo = bare_foo;
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  (foo = bare_foo) = bare_foo;
  EXPECT_EQ(1u, foo->local_constructor_calls);
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  EXPECT_EQ(12342, foo->GetIdentifier());
  EXPECT_EQ(1u, foo->local_method_calls);

  bare_foo = NULL;
  EXPECT_EQ(0u, Foo::global_destructor_calls);

  foo = bare_foo;
  EXPECT_EQ(1u, Foo::global_constructor_calls);
  EXPECT_EQ(1u, Foo::global_destructor_calls);
}

TEST_F(T_UniquePtr, VoidPtr) {
  UniquePtr<void> p(malloc(1024));
  EXPECT_TRUE(p.IsValid());
  EXPECT_NE(static_cast<void*>(NULL), p.weak_ref());

  UniquePtr<void> p2;
  EXPECT_FALSE(p2.IsValid());
  EXPECT_EQ(static_cast<void*>(NULL), p2.weak_ref());
}
