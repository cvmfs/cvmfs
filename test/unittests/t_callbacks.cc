/**
 * This file is part of the CernVM File System.
 */

#include <gtest/gtest.h>

#include "util/async.h"


void CallbackFn(bool * const &param) { *param = true; }

static int callback_fn_void_calls = 0;
void CallbackFnVoid() { ++callback_fn_void_calls; }

TEST(T_Callbacks, SimpleCallback) {
  bool callback_called = false;

  Callback<bool *> callback(&CallbackFn);
  callback(&callback_called);
  EXPECT_TRUE(callback_called);
}

const int closure_data_item = 93142;
struct ClosureData {
  ClosureData() : data(closure_data_item) { }
  int data;
};

class DummyCallbackDelegate {
 public:
  DummyCallbackDelegate() : callback_result(-1) { }
  void CallbackMd(const int &value) { callback_result = value; }
  void CallbackMdVoid() { ++callback_result; }
  void CallbackClosureMd(const int &value, ClosureData data) {
    callback_result = value + data.data;
  }
  void CallbackClosureMdVoid(ClosureData data) { callback_result = data.data; }

 public:
  int callback_result;
};

TEST(T_Callbacks, BoundCallback) {
  DummyCallbackDelegate delegate;
  ASSERT_EQ(-1, delegate.callback_result);

  BoundCallback<int, DummyCallbackDelegate> callback(
      &DummyCallbackDelegate::CallbackMd, &delegate);
  callback(42);
  EXPECT_EQ(42, delegate.callback_result);
}

TEST(T_Callbacks, BoundClosure) {
  DummyCallbackDelegate delegate;
  ASSERT_EQ(-1, delegate.callback_result);

  ClosureData closure_data;
  ASSERT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, delegate.callback_result);

  BoundClosure<int, DummyCallbackDelegate, ClosureData> closure(
      &DummyCallbackDelegate::CallbackClosureMd, &delegate, closure_data);
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, delegate.callback_result);

  closure(1337);
  // didn't change (closure captured copy)
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(closure_data_item + 1337, delegate.callback_result);
}

TEST(T_Callbacks, VoidCallback) {
  Callback<void> callback(&CallbackFnVoid);
  EXPECT_EQ(0, callback_fn_void_calls);
  callback();
  EXPECT_EQ(1, callback_fn_void_calls);
  callback();
  EXPECT_EQ(2, callback_fn_void_calls);
}

TEST(T_Callbacks, VoidBoundCallback) {
  DummyCallbackDelegate delegate;
  ASSERT_EQ(-1, delegate.callback_result);

  BoundCallback<void, DummyCallbackDelegate> callback(
      &DummyCallbackDelegate::CallbackMdVoid, &delegate);
  EXPECT_EQ(-1, delegate.callback_result);
  callback();
  EXPECT_EQ(0, delegate.callback_result);
  callback();
  EXPECT_EQ(1, delegate.callback_result);
  callback();
  EXPECT_EQ(2, delegate.callback_result);
}

TEST(T_Callbacks, VoidBoundClosure) {
  DummyCallbackDelegate delegate;
  ASSERT_EQ(-1, delegate.callback_result);

  ClosureData closure_data;
  ASSERT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, delegate.callback_result);

  BoundClosure<void, DummyCallbackDelegate, ClosureData> closure(
      &DummyCallbackDelegate::CallbackClosureMdVoid, &delegate, closure_data);
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, delegate.callback_result);

  closure();  // didn't change (closure captured copy)
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(closure_data_item, delegate.callback_result);
}


//------------------------------------------------------------------------------


class DummyCallbackable : public Callbackable<int> {
 public:
  DummyCallbackable() : callback_result(-1) { }
  static void CallbackFn(const int &value) { g_callback_result = value; }
  void CallbackMd(const int &value) { callback_result = value; }
  void CallbackClosureMd(const int &value, ClosureData data) {
    callback_result = value + data.data;
  }

 public:
  int callback_result;
  static int g_callback_result;
};
int DummyCallbackable::g_callback_result = -1;

class DummyCallbackableVoid : public Callbackable<void> {
 public:
  DummyCallbackableVoid() : callback_result(-1) { }

  static void CallbackFn() { ++g_void_callback_calls; }
  void CallbackMd() { ++callback_result; }
  void CallbackClosureMd(ClosureData data) { callback_result = data.data; }

 public:
  int callback_result;
  static int g_void_callback_calls;
};
int DummyCallbackableVoid::g_void_callback_calls = 0;

TEST(T_Callbacks, CallbackableCallback) {
  ASSERT_EQ(-1, DummyCallbackable::g_callback_result);

  DummyCallbackable::CallbackTN *callback = DummyCallbackable::MakeCallback(
      &DummyCallbackable::CallbackFn);
  EXPECT_EQ(-1, DummyCallbackable::g_callback_result);

  (*callback)(1337);

  EXPECT_EQ(1337, DummyCallbackable::g_callback_result);
  DummyCallbackable::g_callback_result = -1;
  ASSERT_EQ(-1, DummyCallbackable::g_callback_result);
}

TEST(T_Callbacks, CallbackableBoundCallback) {
  DummyCallbackable callbackable;
  ASSERT_EQ(-1, callbackable.callback_result);

  DummyCallbackable::CallbackTN *callback = DummyCallbackable::MakeCallback(
      &DummyCallbackable::CallbackMd, &callbackable);
  (*callback)(1337);

  EXPECT_EQ(1337, callbackable.callback_result);
}

TEST(T_Callbacks, CallbackableBoundClosure) {
  DummyCallbackable callbackable;
  ASSERT_EQ(-1, callbackable.callback_result);

  ClosureData closure_data;
  ASSERT_EQ(closure_data_item, closure_data.data);

  DummyCallbackable::CallbackTN *callback = DummyCallbackable::MakeClosure(
      &DummyCallbackable::CallbackClosureMd, &callbackable, closure_data);
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, callbackable.callback_result);

  (*callback)(1337);

  // didn't change (closure captured copy)
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(1337 + closure_data_item, callbackable.callback_result);
}

TEST(T_Callbacks, CallbackableVoidCallback) {
  ASSERT_EQ(0, DummyCallbackableVoid::g_void_callback_calls);

  DummyCallbackableVoid::CallbackTN
      *callback = DummyCallbackableVoid::MakeCallback(
          &DummyCallbackableVoid::CallbackFn);
  EXPECT_EQ(0, DummyCallbackableVoid::g_void_callback_calls);

  (*callback)();
  EXPECT_EQ(1, DummyCallbackableVoid::g_void_callback_calls);
  (*callback)();
  EXPECT_EQ(2, DummyCallbackableVoid::g_void_callback_calls);
  (*callback)();
  EXPECT_EQ(3, DummyCallbackableVoid::g_void_callback_calls);

  DummyCallbackableVoid::g_void_callback_calls = 0;
  ASSERT_EQ(0, DummyCallbackableVoid::g_void_callback_calls);
}

TEST(T_Callbacks, CallbackableVoidBoundCallback) {
  DummyCallbackableVoid callbackable;
  ASSERT_EQ(-1, callbackable.callback_result);

  DummyCallbackableVoid::CallbackTN
      *callback = DummyCallbackableVoid::MakeCallback(
          &DummyCallbackableVoid::CallbackMd, &callbackable);
  EXPECT_EQ(-1, callbackable.callback_result);
  (*callback)();
  EXPECT_EQ(0, callbackable.callback_result);
  (*callback)();
  EXPECT_EQ(1, callbackable.callback_result);
  (*callback)();
}

TEST(T_Callbacks, CallbackableVoidBoundClosure) {
  DummyCallbackableVoid callbackable;
  ASSERT_EQ(-1, callbackable.callback_result);

  ClosureData closure_data;
  ASSERT_EQ(closure_data_item, closure_data.data);

  DummyCallbackableVoid::CallbackTN
      *callback = DummyCallbackableVoid::MakeClosure(
          &DummyCallbackableVoid::CallbackClosureMd,
          &callbackable,
          closure_data);
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(-1, callbackable.callback_result);

  (*callback)();

  // didn't change (closure captured copy)
  EXPECT_EQ(closure_data_item, closure_data.data);
  EXPECT_EQ(closure_data_item, callbackable.callback_result);
}
