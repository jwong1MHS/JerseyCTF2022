/* Replacement of AntiWord's fail.h by Sherlock ASSERT. MJ, 2004. */

#if !defined(__fail_h)
#define __fail_h 1

#define fail(e) ASSERT(!(e))

#endif /* __fail_h */
