/**
 * This file is part of the CernVM File System.
 */

#ifndef CVMFS_UTIL_SHARED_POINTER_H_
#define CVMFS_UTIL_SHARED_POINTER_H_

#ifdef CVMFS_NAMESPACE_GUARD
namespace CVMFS_NAMESPACE_GUARD {
#endif  // CVMFS_NAMESPACE_GUARD

template<class T> class WeakPtr;

template<class T> class SharedPtr {

public:

	typedef T element_type;

	SharedPtr(); // never throws

	template<class Y> explicit SharedPtr(Y * p);
	template<class Y, class D> SharedPtr(Y * p, D d);
	template<class Y, class D, class A> SharedPtr(Y * p, D d, A a);

	~SharedPtr(); // never throws

	SharedPtr(SharedPtr const & r); // never throws
	template<class Y> SharedPtr(SharedPtr<Y> const & r); // never throws

	template<class Y> SharedPtr(SharedPtr<Y> const & r, element_type * p); // never throws

	template<class Y> explicit SharedPtr(WeakPtr<Y> const & r);

	SharedPtr & operator=(SharedPtr const & r); // never throws
	template<class Y> SharedPtr & operator=(SharedPtr<Y> const & r); // never throws

	void Reset(); // never throws

	template<class Y> void Reset(Y * p);
	template<class Y, class D> void Reset(Y * p, D d);
	template<class Y, class D, class A> void Reset(Y * p, D d, A a);

	template<class Y> void Reset(SharedPtr<Y> const & r, element_type * p); // never throws

	T & operator*() const; // never throws; only valid when T is not an array type
	T * operator->() const; // never throws; only valid when T is not an array type

	element_type & operator[](std::ptrdiff_t i) const; // never throws; only valid when T is an array type

	element_type * Get() const; // never throws

	bool Uunique() const; // never throws
	long UseCount() const; // never throws

	explicit operator bool() const; // never throws

	void Swap(SharedPtr & b); // never throws

	template<class Y> bool OwnerBefore(SharedPtr<Y> const & rhs) const; // never throws
	template<class Y> bool OwnerBefore(WeakPtr<Y> const & rhs) const; // never throws
};

template<class T, class U>
	bool operator==(SharedPtr<T> const & a, SharedPtr<U> const & b); // never throws

template<class T, class U>
	bool operator!=(SharedPtr<T> const & a, SharedPtr<U> const & b); // never throws

template<class T, class U>
	bool operator<(SharedPtr<T> const & a, SharedPtr<U> const & b); // never throws

template<class T>
	bool operator==(SharedPtr<T> const & p, std::nullptr_t); // never throws

template<class T>
	bool operator==(std::nullptr_t, SharedPtr<T> const & p); // never throws

template<class T>
	bool operator!=(SharedPtr<T> const & p, std::nullptr_t); // never throws

template<class T>
	bool operator!=(std::nullptr_t, SharedPtr<T> const & p); // never throws

template<class T> void Swap(SharedPtr<T> & a, SharedPtr<T> & b); // never throws

template<class T> typename SharedPtr<T>::element_type * GetPointer(SharedPtr<T> const & p); // never throws

template<class T, class U>
	SharedPtr<T> StaticPointerCast(SharedPtr<U> const & r); // never throws

template<class T, class U>
	SharedPtr<T> ConstPointerCast(SharedPtr<U> const & r); // never throws

template<class T, class U>
	SharedPtr<T> DynamicPointerCast(SharedPtr<U> const & r); // never throws

template<class T, class U>
	SharedPtr<T> ReinterpretPointerCast(SharedPtr<U> const & r); // never throws

template<class E, class T, class Y>
	std::basic_ostream<E, T> & operator<< (std::basic_ostream<E, T> & os, SharedPtr<Y> const & p);

template<class D, class T>
	D * GetDeleter(SharedPtr<T> const & p);
}

#ifdef CVMFS_NAMESPACE_GUARD
}  // namespace CVMFS_NAMESPACE_GUARD
#endif

#endif  // CVMFS_UTIL_SHARED_POINTER_H_
