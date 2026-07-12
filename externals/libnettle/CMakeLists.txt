# CMake module for the nettle low-level crypto library.
include(FetchContent)
include(CheckCSourceCompiles)
include(CheckSymbolExists)
include(CheckTypeSize)
include(TestBigEndian)

set(NETTLE_VERSION "3.10.2")
set(NETTLE_URL "https://ftp.gnu.org/gnu/nettle/nettle-${NETTLE_VERSION}.tar.gz")
set(NETTLE_URL_MIRROR "https://ecsft.cern.ch/dist/cvmfs/build_externals/nettle-${NETTLE_VERSION}.tar.gz")
set(NETTLE_HASH "MD5=b28bcbf6f045ff007940a9401673600d")

# -> nettle <-
# A system nettle >= 3.10 is preferred; the floor is SHAKE128 (sha3_128_shake),
# which cvmfs/crypto uses and 3.10 introduced. Otherwise build the vendored
# tarball: rather than running nettle's autotools build, compile its sources
# into a static libnettle.a with a generated config.h. No public-key code
# (hogweed / GMP) -- cvmfs only uses md5, sha1, sha2, ripemd160 and aes/cbc.
# File lists and assembler selection are read out of nettle's Makefile.in and
# configure.ac, so a version bump picks up upstream's changes.

# nettle_make_list(<out> <makefile> <var> <suffix_regex> <min_expected>)
#
# Files listed in make variable <var> matching <suffix_regex>, backslash
# continuations unfolded. Literal assignments only -- @configure@ and $(...)
# are not expanded, which is all nettle_SOURCES and HEADERS need.
function(nettle_make_list out makefile var suffix_regex min_expected)
  file(READ "${makefile}" _mk)
  string(REGEX REPLACE "\\\\\n" " " _mk "${_mk}")
  if (NOT _mk MATCHES "\n${var} *=([^\n]*)")
    message(FATAL_ERROR "nettle: no '${var}' assignment found in ${makefile}")
  endif ()
  set(_assignment "${CMAKE_MATCH_1}")
  if (_assignment MATCHES "[@$]")
    message(FATAL_ERROR
      "nettle: '${var}' in ${makefile} contains make/configure substitutions "
      "that this parser cannot expand: ${_assignment}")
  endif ()
  string(REGEX MATCHALL "[A-Za-z0-9_+-]+${suffix_regex}" _files "${_assignment}")
  list(LENGTH _files _count)
  if (_count LESS ${min_expected})
    message(FATAL_ERROR
      "nettle: only ${_count} files parsed from '${var}' in ${makefile}, "
      "expected at least ${min_expected} -- the file list format has changed")
  endif ()
  set(${out} ${_files} PARENT_SCOPE)
endfunction()

# nettle_shell_list(<out> <file> <var> <suffix_regex>)
#
# Same idea, for configure.ac's shell assignments (asm_replace_list,
# asm_nettle_optional_list) recording which .asm files may be used.
function(nettle_shell_list out file var suffix_regex)
  file(READ "${file}" _sh)
  string(REGEX REPLACE "\\\\\n" " " _sh "${_sh}")
  if (NOT _sh MATCHES "\n${var}=\"([^\"]*)\"")
    message(FATAL_ERROR "nettle: no '${var}' assignment found in ${file}")
  endif ()
  string(REGEX MATCHALL "[A-Za-z0-9_+-]+${suffix_regex}" _files "${CMAKE_MATCH_1}")
  set(${out} ${_files} PARENT_SCOPE)
endfunction()

# nettle_asm_find(<out> <name>)
#
# Resolve <name> against NETTLE_ASM_DIRS, first match wins. Ordering matters:
# fat/ comes first so its dispatching variants win.
function(nettle_asm_find out name)
  foreach(_dir ${NETTLE_ASM_DIRS})
    if (EXISTS "${nettle_SOURCE_DIR}/${_dir}/${name}")
      set(${out} "${nettle_SOURCE_DIR}/${_dir}/${name}" PARENT_SCOPE)
      return()
    endif ()
  endforeach()
  set(${out} "NOTFOUND" PARENT_SCOPE)
endfunction()

# nettle_try_assemble(<snippet> <outvar>)
#
# Write the snippet to a .s file and see whether the compiler driver accepts
# it. Same compiler and flags as the rest of the build, so a cross-toolchain
# gets the right answers.
function(nettle_try_assemble snippet outvar)
  if (DEFINED ${outvar})
    return()
  endif ()
  set(_probe "${CMAKE_CURRENT_BINARY_DIR}/asm-probe/${outvar}.s")
  file(WRITE "${_probe}" "${snippet}\n")
  separate_arguments(_flags NATIVE_COMMAND "${CMAKE_C_FLAGS}")
  execute_process(
    COMMAND "${CMAKE_C_COMPILER}" ${_flags} -c "${_probe}"
            -o "${_probe}.o"
    RESULT_VARIABLE _rc OUTPUT_QUIET ERROR_QUIET)
  if (_rc EQUAL 0)
    set(${outvar} "yes" CACHE INTERNAL "nettle asm probe: ${outvar}")
  else ()
    set(${outvar} "no" CACHE INTERNAL "nettle asm probe: ${outvar}")
  endif ()
  message(STATUS "nettle asm probe ${outvar}: ${${outvar}}")
endfunction()

setup_external_build_mode(nettle)

FetchContent_Declare(
  Nettle
  URL "${NETTLE_URL}" "${NETTLE_URL_MIRROR}"
  URL_HASH "${NETTLE_HASH}"
  DOWNLOAD_EXTRACT_TIMESTAMP TRUE
  DOWNLOAD_DIR ${EXTERNALS_DOWNLOAD_LOCATION}
  INSTALL_DIR ${EXTERNALS_INSTALL_LOCATION}
  EXCLUDE_FROM_ALL
  # Resolved via cmake/Modules/FindNettle.cmake. The tarball has no
  # CMakeLists.txt, so on fallback MakeAvailable only extracts and the target
  # is defined in the else() below.
  FIND_PACKAGE_ARGS 3.10
)

FetchContent_MakeAvailable(Nettle)
unset(FETCHCONTENT_TRY_FIND_PACKAGE_MODE)

if (Nettle_FOUND)
  message(STATUS "Found system nettle ${Nettle_VERSION}: ${NETTLE_LIBRARY}")
  # Promote FindNettle's NETTLE_* results to the Libnettle_* names the tree
  # consumes (cached: this runs from the externals/ subdirectory).
  set(Libnettle_LIBRARIES    "${NETTLE_LIBRARY}"     CACHE INTERNAL "nettle library")
  set(Libnettle_INCLUDE_DIRS "${NETTLE_INCLUDE_DIR}" CACHE INTERNAL "nettle include dir")
else ()
  message(STATUS "Building nettle from vendored sources in ${nettle_SOURCE_DIR}")

  # --- generate config.h (replaces nettle's ./configure output) -------------
  check_type_size("long"   SIZEOF_LONG)
  check_type_size("size_t" SIZEOF_SIZE_T)
  test_big_endian(NETTLE_IS_BIG_ENDIAN)

  set(HAVE_ALLOCA         1)
  set(HAVE_ALLOCA_H       1)
  set(HAVE_BUILTIN_BSWAP64 1)
  set(HAVE_GCC_ATTRIBUTE  1)
  set(HAVE_INTTYPES_H     1)
  set(HAVE_MEMORY_H       1)
  set(HAVE_STDINT_H       1)
  set(HAVE_STDLIB_H       1)
  set(HAVE_STRERROR       1)
  set(HAVE_STRINGS_H      1)
  set(HAVE_STRING_H       1)
  set(HAVE_SYS_STAT_H     1)
  set(HAVE_SYS_TYPES_H    1)
  set(HAVE_UNISTD_H       1)
  set(STDC_HEADERS        1)
  set(TIME_WITH_SYS_TIME  1)
  set(WORDS_BIGENDIAN     ${NETTLE_IS_BIG_ENDIAN})

  # Fat dispatchers only: secure_getenv guards NETTLE_FAT_OVERRIDE,
  # elf_aux_info is the *BSD getauxval. _GNU_SOURCE declares secure_getenv.
  set(CMAKE_REQUIRED_DEFINITIONS "-D_GNU_SOURCE")
  check_symbol_exists(secure_getenv "stdlib.h"  HAVE_SECURE_GETENV)
  check_symbol_exists(elf_aux_info  "sys/auxv.h" HAVE_ELF_AUX_INFO)
  unset(CMAKE_REQUIRED_DEFINITIONS)

  configure_file(${CMAKE_CURRENT_SOURCE_DIR}/nettle-config.h.in
                 ${CMAKE_CURRENT_BINARY_DIR}/config.h)

  # --- generate version.h (from nettle's own version.h.in) ------------------
  set(MAJOR_VERSION 3)
  set(MINOR_VERSION 10)
  # Only hogweed would need GMP; version.h guards GMP_NUMB_BITS on this.
  set(NETTLE_USE_MINI_GMP 0)
  set(GMP_NUMB_BITS 0)
  configure_file(${nettle_SOURCE_DIR}/version.h.in
                 ${CMAKE_CURRENT_BINARY_DIR}/version.h)

  # --- assembler selection ---------------------------------------------------
  # Mirrors configure.ac's asm_path / asm_replace_list / asm_nettle_optional_list
  # logic. Always the "fat" flavour: fat-x86_64.c / fat-arm64.c pick between the
  # C and optimised routines from the CPU feature bits at run time. The fast
  # paths need non-baseline extensions (SHA-NI / AES-NI, ARMv8 crypto), so
  # compile-time selection would SIGILL on older CPUs.
  option(NETTLE_ENABLE_ASSEMBLER "Build nettle's optimised assembler routines" ON)

  set(NETTLE_ASM_DIRS "")
  set(NETTLE_FAT_SOURCE "")
  if (NETTLE_ENABLE_ASSEMBLER AND CMAKE_SIZEOF_VOID_P EQUAL 8)
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64)$")
      set(NETTLE_ASM_DIRS "x86_64/fat" "x86_64")
      set(NETTLE_FAT_SOURCE "fat-x86_64.c")
    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
      # NB: not a "^arm" glob -- the tree's "^arm.*$" misses aarch64.
      set(NETTLE_ASM_DIRS "arm64/fat" "arm64")
      set(NETTLE_FAT_SOURCE "fat-arm64.c")
    endif ()
  endif ()

  # The .asm files are m4 templates; without m4 fall back to portable C.
  if (NETTLE_ASM_DIRS)
    find_program(M4_EXECUTABLE m4)
    if (NOT M4_EXECUTABLE)
      message(WARNING "nettle: m4 not found, building without assembler "
                      "optimisations (expect ~4x slower SHA-1 hashing)")
      set(NETTLE_ASM_DIRS "")
    endif ()
  endif ()

  set(NETTLE_ASM_SOURCES "")
  set(NETTLE_ASM_REPLACED "")
  set(NETTLE_NATIVE_FUNCS "")

  if (NETTLE_ASM_DIRS)
    enable_language(ASM)

    # config.m4 -- m4 counterpart of config.h, the target's assembler dialect.
    nettle_try_assemble(
      ".text\n.globl foo\n.type foo,%function\nfoo:\n.Lend:\n.size foo, .Lend - foo"
      NETTLE_ASM_TYPE_PERCENT)
    nettle_try_assemble(
      ".text\n.globl foo\n.type foo,#function\nfoo:\n.Lend:\n.size foo, .Lend - foo"
      NETTLE_ASM_TYPE_HASH)
    nettle_try_assemble(".align 3" NETTLE_ASM_ALIGN_LOG)

    set(ASM_ELF_STYLE "no")
    set(ASM_TYPE_FUNCTION "@function")
    set(ASM_TYPE_PROGBITS "@progbits")
    if (NETTLE_ASM_TYPE_PERCENT STREQUAL "yes")
      set(ASM_ELF_STYLE "yes")
      set(ASM_TYPE_FUNCTION "%function")
      set(ASM_TYPE_PROGBITS "%progbits")
    elseif (NETTLE_ASM_TYPE_HASH STREQUAL "yes")
      set(ASM_ELF_STYLE "yes")
      set(ASM_TYPE_FUNCTION "#function")
      set(ASM_TYPE_PROGBITS "#progbits")
    endif ()

    set(srcdir "${nettle_SOURCE_DIR}")
    set(ASM_COFF_STYLE "no")          # no Windows target
    set(ASM_ALIGN_LOG "${NETTLE_ASM_ALIGN_LOG}")
    set(ELFV2_ABI "no")               # powerpc64 only
    set(W64_ABI "no")                 # Windows only
    set(ASM_PPC_WANT_R_REGISTERS "n/a")
    if (NETTLE_IS_BIG_ENDIAN)
      set(ASM_WORDS_BIGENDIAN "yes")
    else ()
      set(ASM_WORDS_BIGENDIAN "no")
    endif ()
    if (APPLE)
      set(ASM_SYMBOL_PREFIX "_")
      set(ASM_RODATA ".section __TEXT,__const")
      set(ASM_MARK_NOEXEC_STACK "")
    else ()
      set(ASM_SYMBOL_PREFIX "")
      set(ASM_RODATA ".section .rodata")
      set(ASM_MARK_NOEXEC_STACK ".section .note.GNU-stack,\"\",TYPE_PROGBITS")
    endif ()
    # Intel CET (x86 only), gated on -fcf-protection (which defines __CET__),
    # NOT on the assembler understanding endbr64. Distro RPM builds enable it
    # by default, and assembler lacking the .note.gnu.property marking makes
    # the linker downgrade the *whole* binary to non-CET.
    set(ASM_X86_ENDBR "")
    set(ASM_X86_MARK_CET_ALIGN "")
    set(ASM_X86_MARK_CET "")
    if (NETTLE_FAT_SOURCE STREQUAL "fat-x86_64.c")
      check_c_source_compiles(
        "#ifndef __CET__\n#error Intel CET is not enabled\n#endif\nint main(void){return 0;}"
        NETTLE_ASM_X86_INTEL_CET)
      if (NETTLE_ASM_X86_INTEL_CET)
        # ABI is always 64 here: NETTLE_ASM_DIRS is only set for 8-byte pointers.
        set(ASM_X86_ENDBR "endbr64")
        set(ASM_X86_MARK_CET_ALIGN "3")
        check_c_source_compiles(
          "#if !defined __ELF__ || !defined __CET__\n#error GNU property is not needed\n#endif\nint main(void){return 0;}"
          NETTLE_ASM_X86_GNU_PROPERTY)
        if (NETTLE_ASM_X86_GNU_PROPERTY)
          # Verbatim from configure.ac; ASM_X86_MARK_CET_ALIGN stays an m4
          # reference, resolved from config.m4 on expansion.
          set(ASM_X86_MARK_CET "
\t.pushsection \".note.gnu.property\", \"a\"
\t.p2align ASM_X86_MARK_CET_ALIGN
\t.long 1f - 0f
\t.long 4f - 1f
\t.long 5
0:
\t.asciz \"GNU\"
1:
\t.p2align ASM_X86_MARK_CET_ALIGN
\t.long 0xc0000002
\t.long 3f - 2f
2:
\t.long 3
3:
\t.p2align ASM_X86_MARK_CET_ALIGN
4:
\t.popsection")
        endif ()
      endif ()
    endif ()

    configure_file(${nettle_SOURCE_DIR}/config.m4.in
                   ${CMAKE_CURRENT_BINARY_DIR}/config.m4 @ONLY)

    # machine.m4 is arch-specific and included by every .asm translation.
    nettle_asm_find(_machine_m4 "machine.m4")
    configure_file(${_machine_m4} ${CMAKE_CURRENT_BINARY_DIR}/machine.m4 COPYONLY)

    set(_m4_deps
      ${nettle_SOURCE_DIR}/m4-utils.m4
      ${nettle_SOURCE_DIR}/asm.m4
      ${CMAKE_CURRENT_BINARY_DIR}/config.m4
      ${CMAKE_CURRENT_BINARY_DIR}/machine.m4)

    # Run the m4 pipeline for one .asm and append the .s to NETTLE_ASM_SOURCES.
    macro(nettle_add_asm _asm_path _stem)
      set(_s "${CMAKE_CURRENT_BINARY_DIR}/asm/${_stem}.s")
      add_custom_command(
        OUTPUT "${_s}"
        COMMAND ${CMAKE_COMMAND} -E make_directory "${CMAKE_CURRENT_BINARY_DIR}/asm"
        COMMAND ${M4_EXECUTABLE} ${_m4_deps} "${_asm_path}" > "${_s}"
        DEPENDS "${_asm_path}" ${_m4_deps}
        COMMENT "Preprocessing nettle asm ${_stem}"
        VERBATIM)
      list(APPEND NETTLE_ASM_SOURCES "${_s}")
    endmacro()

    # Replacement routines: each .asm supersedes the .c of the same stem,
    # which must be dropped from the C source list.
    nettle_shell_list(NETTLE_ASM_REPLACE_LIST "${nettle_SOURCE_DIR}/configure.ac"
                      asm_replace_list "[.]asm")
    foreach(_a ${NETTLE_ASM_REPLACE_LIST})
      nettle_asm_find(_found "${_a}")
      if (_found)
        string(REGEX REPLACE "[.]asm$" "" _stem "${_a}")
        nettle_add_asm("${_found}" "${_stem}")
        list(APPEND NETTLE_ASM_REPLACED "${_stem}.c")
      endif ()
    endforeach()

    # Optional routines add an implementation instead of replacing a .c file.
    # HAVE_NATIVE_<func> makes the C version compile under a _c suffix so the
    # fat dispatcher can choose between them at run time.
    nettle_shell_list(NETTLE_ASM_OPTIONAL_LIST "${nettle_SOURCE_DIR}/configure.ac"
                      asm_nettle_optional_list "[.]asm")
    foreach(_a ${NETTLE_ASM_OPTIONAL_LIST})
      nettle_asm_find(_found "${_a}")
      if (_found)
        string(REGEX REPLACE "[.]asm$" "" _stem "${_a}")
        nettle_add_asm("${_found}" "${_stem}")
        # configure derives the HAVE_NATIVE_* names by scraping PROLOGUE();
        # some fat stubs carry a dnl-commented PROLOGUE purely to be found.
        file(STRINGS "${_found}" _prologues REGEX "PROLOGUE\\(")
        foreach(_line ${_prologues})
          if (_line MATCHES "PROLOGUE\\(_*(nettle_)?([A-Za-z0-9_]+)\\)")
            list(APPEND NETTLE_NATIVE_FUNCS "${CMAKE_MATCH_2}")
          endif ()
        endforeach()
      endif ()
    endforeach()

    # ...but only HAVE_NATIVE_* macros with a template in config.h.in take
    # effect under autotools (AC_DEFINE_UNQUOTED on an undeclared name is
    # dropped), so intersect -- chacha_2core is in, chacha_2core32 is not.
    file(READ "${nettle_SOURCE_DIR}/config.h.in" _cfg_in)
    string(REGEX MATCHALL "HAVE_NATIVE_[A-Za-z0-9_]+" _known "${_cfg_in}")
    list(REMOVE_DUPLICATES _known)
    set(_natives "")
    foreach(_f ${NETTLE_NATIVE_FUNCS})
      if ("HAVE_NATIVE_${_f}" IN_LIST _known)
        list(APPEND _natives "HAVE_NATIVE_${_f}")
      endif ()
    endforeach()
    list(REMOVE_DUPLICATES _natives)
    set(NETTLE_NATIVE_DEFINES ${_natives})

    list(LENGTH NETTLE_ASM_SOURCES _n_asm)
    message(STATUS "nettle: ${_n_asm} assembler routines for "
                   "${CMAKE_SYSTEM_PROCESSOR} (runtime CPU dispatch)")
  else ()
    set(NETTLE_NATIVE_DEFINES "")
    message(STATUS "nettle: portable C build, no assembler optimisations")
  endif ()

  # --- the static library ----------------------------------------------------
  # `nettle_SOURCES` is libnettle.a's C files in the assembler-free config.
  nettle_make_list(NETTLE_SOURCES "${nettle_SOURCE_DIR}/Makefile.in"
                   nettle_SOURCES "[.]c" 200)

  # Drop the C files superseded by an assembler implementation, and add the
  # fat dispatcher (configure carries it in OPT_NETTLE_SOURCES).
  if (NETTLE_ASM_REPLACED)
    list(REMOVE_ITEM NETTLE_SOURCES ${NETTLE_ASM_REPLACED})
  endif ()
  if (NETTLE_FAT_SOURCE AND NETTLE_ASM_DIRS)
    list(APPEND NETTLE_SOURCES "${NETTLE_FAT_SOURCE}")
  endif ()
  list(TRANSFORM NETTLE_SOURCES PREPEND "${nettle_SOURCE_DIR}/")

  add_library(nettle STATIC EXCLUDE_FROM_ALL
              ${NETTLE_SOURCES} ${NETTLE_ASM_SOURCES})
  target_compile_definitions(nettle PRIVATE HAVE_CONFIG_H ${NETTLE_NATIVE_DEFINES})
  # nettle's sources need the generated config.h/version.h (binary dir) and
  # their flat siblings (source dir); consumers see only <nettle/...>.
  target_include_directories(nettle
    PUBLIC
      $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/include>
      $<INSTALL_INTERFACE:include>
    PRIVATE
      ${nettle_SOURCE_DIR}
      ${CMAKE_CURRENT_BINARY_DIR}
  )
  set_target_properties(nettle PROPERTIES POSITION_INDEPENDENT_CODE ON)

  # --- public headers for consumers -----------------------------------------
  # nettle ships headers flat in the source root; reproduce the installed
  # <nettle/...> layout. `HEADERS` covers both libraries, so subtract the
  # hogweed ones, which would advertise APIs this build lacks. Excluding rather
  # than including means new symmetric/hash headers arrive with a version bump,
  # while a new public-key header has to be added here deliberately.
  nettle_make_list(NETTLE_PUBLIC_HEADERS "${nettle_SOURCE_DIR}/Makefile.in"
                   HEADERS "[.]h" 50)
  list(REMOVE_ITEM NETTLE_PUBLIC_HEADERS
    asn1.h  bignum.h  curve25519.h  curve448.h
    dsa-compat.h  dsa.h  ecc-curve.h  ecc.h
    ecdsa.h  eddsa.h  gostdsa.h  pgp.h
    pkcs1.h  pss-mgf1.h  pss.h  rsa.h
    sexp.h
  )
  set(NETTLE_PUBLIC_HEADER_PATHS "")
  foreach(_h ${NETTLE_PUBLIC_HEADERS})
    list(APPEND NETTLE_PUBLIC_HEADER_PATHS "${nettle_SOURCE_DIR}/${_h}")
  endforeach()
  file(COPY ${NETTLE_PUBLIC_HEADER_PATHS}
       DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/include/nettle)
  configure_file(${nettle_SOURCE_DIR}/version.h.in
                 ${CMAKE_CURRENT_BINARY_DIR}/include/nettle/version.h)

  # Same names as the Nettle_FOUND branch above.
  add_library(Nettle::Nettle ALIAS nettle)
  set(Libnettle_LIBRARIES    nettle                                CACHE INTERNAL "nettle library")
  set(Libnettle_INCLUDE_DIRS "${CMAKE_CURRENT_BINARY_DIR}/include" CACHE INTERNAL "nettle include dir")

  install(TARGETS nettle DESTINATION ${EXTERNALS_INSTALL_LOCATION} COMPONENT external_deps EXCLUDE_FROM_ALL)
  install(DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/include/nettle
          DESTINATION ${EXTERNALS_INSTALL_LOCATION}/include COMPONENT external_deps EXCLUDE_FROM_ALL)
  set(EXTERNALS_INSTALL_TARGETS ${EXTERNALS_INSTALL_TARGETS} "nettle" PARENT_SCOPE)
endif ()
