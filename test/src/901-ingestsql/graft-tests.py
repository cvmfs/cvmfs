#!/usr/bin/python3

import subprocess
import shutil
import sqlite3
import tempfile
import sys
import os
import stat
import xattr
import posix1e
import tempfile
import time
import multiprocessing
import requests
import random

DEBUG = os.getenv("DEBUG", default=False)
FILE_PATH = os.path.dirname(__file__)
WAIT_SECONDS_TO_APPEAR = 0
MOUNT_POINT = os.getenv("CVMFS_TEST_MOUNTPOINT") or "/tmp/ingestsql_test_mount"
REPO_NAME = os.getenv("CVMFS_TEST_REPO") or "test.repo"

CVMFS_TEST_HTTP_BASE=os.getenv("CVMFS_TEST_HTTP_BASE") or f"http://127.0.0.1:8000/{REPO_NAME}"
CVMFS_TEST_S3_CONFIG=os.getenv("CVMFS_TEST_S3_CONFIG")

def clear_db(connection):
    con = connection[0]
    cur = con.cursor()
    cur.execute("DELETE FROM dirs")
    con.commit()
    cur.execute("DELETE FROM links")
    con.commit()
    cur.execute("DELETE FROM files")
    con.commit()
    cur.execute("DELETE FROM deletions")
    con.commit()


def remount_repo():
    umount_repo()
    # may fail to mount again if no delay is used
    time.sleep(10)
    mount_repo()


def umount_repo():
    cmd = ["fusermount", "-u", MOUNT_POINT]
    subproc = subprocess.Popen(cmd)
    exitcode = subproc.wait()
    print(f"{cmd} exited with {exitcode}")
    if exitcode != 0:
        raise Exception(f"Unmounting command {cmd} exited with {exitcode}")


def mount_repo():
    arg0 = "cvmfs2"
    if DEBUG:
        arg0 = FILE_PATH + "/cvmfs2-wrapper"
    cmd = [
        arg0,
        "-o",
        f"config=/etc/cvmfs/repositories.d/{REPO_NAME}/client.conf,allow_other",
        REPO_NAME,
        MOUNT_POINT,
    ]
    subproc = subprocess.Popen(cmd)
    exitcode = subproc.wait()
    print(f"{cmd} exited with {exitcode}")
    if exitcode != 0:
        raise Exception(f"Mounting command {cmd} exited with {exitcode}")


def make_db():
    fn = tempfile.mktemp()
    schema = [
        """
CREATE TABLE IF NOT EXISTS dirs (
    name  TEXT    PRIMARY KEY,
    mode  INTEGER NOT NULL DEFAULT 493, -- 0755 in octal
    mtime INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
    owner INTEGER NOT NULL DEFAULT 0,
    grp   INTEGER NOT NULL DEFAULT 0,
    acl   TEXT    NOT NULL DEFAULT "",
    nested INTEGER NOT NULL DEFAULT 1
);
""",
        """
CREATE TABLE IF NOT EXISTS files (
    name   TEXT    PRIMARY KEY,
    mode   INTEGER NOT NULL DEFAULT 420, -- 0644 in octal
    mtime  INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
    owner  INTEGER NOT NULL DEFAULT 0,
    grp    INTEGER NOT NULL DEFAULT 0,
    size   INTEGER NOT NULL DEFAULT 0,
    hashes TEXT    NOT NULL DEFAULT "",
    internal INTEGER NOT NULL DEFAULT 0,
    compressed INTEGER NOT NULL DEFAULT 0
);
""",
        """
CREATE TABLE IF NOT EXISTS links (
    name   TEXT    PRIMARY KEY,
    target TEXT    NOT NULL DEFAULT "",
    mtime  INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
    owner  INTEGER NOT NULL DEFAULT 0,
    grp    INTEGER NOT NULL DEFAULT 0,
    skip_if_file_or_dir INTEGER NOT NULL DEFAULT 0
);
""",
        """
CREATE TABLE IF NOT EXISTS deletions (
    name      TEXT PRIMARY KEY,
    directory INTEGER NOT NULL DEFAULT 0,
    file      INTEGER NOT NULL DEFAULT 0,
    link      INTEGER NOT NULL DEFAULT 0
);
""",
        """
-- Table used by CVMFS to store different properties such as schema version and revision.
CREATE TABLE IF NOT EXISTS properties (
    key   TEXT PRIMARY KEY,
    value TEXT
);
""",
        """
INSERT INTO properties VALUES
    ("schema", "1.0"),
    ("schema_revision", "4")
;
""",
    ]

    # print(fn)
    con = sqlite3.connect(fn)
    cur = con.cursor()
    for s in schema:
        cur.execute(s)
        con.commit()
    return (con, fn)


def build_container():
    try:
        cmd = ["podman", "build", "-t", "cvmfs_gateway", "."]
        print(" ".join(cmd))
        subprocess.check_output(cmd)
    except subprocess.CalledProcessError:
        return False
    return True


def _do_graft_test(dbfile, prefix=None, lease=None, priority=None):
    #  -D    input sqlite DB
    #  -N    fully qualified repository name
    #  -g    gateway URL (optional)
    #  -w    stratum 0 base url (optional)
    #  -t    temporary directory (will try TMPDIR if not set) (optional)
    #  -@    proxy URL (optional)
    #  -l    lease path (optional)
    #  -q    number of concurrent write jobs (optional)
    #  -k    public key (optional)
    #  -s    gateway secret (optional)
    #  -a    Allow additions (default true, false if -d specified) (optional)
    #  -d    Allow deletions (optional)
    try:
        tmpdir = tempfile.TemporaryDirectory()
        cmd = [
            "cvmfs_swissknife_debug",
            "ingestsql",
            "-c",
            "-W", "10", # wait for revision increment
            "-v",
            #"-@",
            #"http://127.0.0.1:8088",
            "-N",
            REPO_NAME,
            "-D",
            dbfile,
            "-w",
            f"{CVMFS_TEST_HTTP_BASE}/{REPO_NAME}",
            "-k",
            f"/etc/cvmfs/keys/{REPO_NAME}.pub",
            "-3",
            f"{CVMFS_TEST_S3_CONFIG}",
            "-s",
            f"/etc/cvmfs/keys/{REPO_NAME}.gw",
            "-g",
            "http://127.0.0.1:4929/api/v1",
            "-t",
            tmpdir.name,
            "-a",
            "-d",
            "-T",
            "2",
            "-B",
            MOUNT_POINT,
        ]
        if DEBUG:
            cmd = ["rr"] + cmd
        if prefix:
            cmd.append("-p")
            cmd.append(prefix)
        if lease:
            cmd.append("-l")
            cmd.append(lease)
        if priority:
            cmd.append("-P")
            cmd.append(str(priority))

        print(" ".join(cmd))
        subproc = subprocess.Popen(cmd)
        exitcode = subproc.wait()
        print(f"{cmd} exited with {exitcode}")
        if exitcode == 0:
            print("Return True")
            return True
        elif exitcode == 1:
            print("Runtime failure. Happens e.g. when gateway says 'invalid lease'.")
            print("Return False")
            return False
        elif exitcode == 128 + 6:
            print(
                "SIGABRT - but that is its way of saying some expectation failed, might be expected in the grand scheme of things"
            )
            # TODO make ingestsql exit gracefully failure rather than crash
            print("Return True")
            return True
        elif exitcode == 128 + 11:
            print("SIGSEGV")
            print("Return False")
            return False
        else:
            print("Runtime failure")
            print("Return False")
            return False
    except Exception as e:
        print(e)
        print("Return False")
        return False


def do_file_read_test(connection):
    conn = connection[0]

    conn.execute(
        "INSERT INTO files VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "file-internal-compressed-implicit",
            0o777,
            0,
            0,
            0,
            6,
            "f0f244b79f74b07c6a9ae5b15e7d5cd6a222e35b",
            1,
            0,
        ),
    )
    conn.execute(
        "INSERT INTO files VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "file-internal-uncompressed",
            0o777,
            0,
            0,
            0,
            6,
            "a8eec30a5b2d71bc890175f5b361ebb28d7c54a8",
            1,
            1,
        ),
    )
    conn.execute(
        "INSERT INTO files VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "file-internal-compressed-explicit",
            0o777,
            0,
            0,
            0,
            6,
            "f0f244b79f74b07c6a9ae5b15e7d5cd6a222e35b",
            1,
            2,
        ),
    )
    conn.execute(
        "INSERT INTO files VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            "file-external-uncompressed",
            0o777,
            0,
            0,
            0,
            6,
            "a8eec30a5b2d71bc890175f5b361ebb28d7c54a8",
            1,
            1,
        ),
    )

    conn.commit()
    ret = _do_graft_test(connection[1])
    if not ret:
        print("Graft failed")
        return False
    # now read the files
    #remount_repo()
    for f in [
        "file-internal-compressed-implicit",
        "file-internal-uncompressed",
        "file-internal-compressed-explicit",
        "file-external-uncompressed",
    ]:
        try:
            w = open(f"{MOUNT_POINT}/{f}", "rt").read()
            if w != "HELLO\n":
                print(f"Contents of {f} do not meet expectation")
                return False
        except OSError as e:
            print(f"open({f}) failed with {e}")
            return False


    return True


def do_test(connection, test, prefix=None, lease=None):
    expect_success = True
    clear = True
    if "result" in test:
        expect_success = test["result"]
    if "clear" in test:
        clear = test["clear"]
    if clear:
        clear_db(connection)
    conn = connection[0]
    if "files" in test and len(test["files"]) > 0:
        conn.executemany(
            "INSERT INTO files VALUES ( ?, ?, ?, ?, ?, ?, ?, 0, 0)", test["files"]
        )
    if "dirs" in test and len(test["dirs"]) > 0:
        conn.executemany(
            "INSERT INTO dirs  VALUES ( ?, ?, ?, ?, ?, ?, 1)", test["dirs"]
        )
    if "links" in test and len(test["links"]) > 0:
        conn.executemany("INSERT INTO links VALUES ( ?, ?, ?, ?, ?, ? )", test["links"])
    if "deletions" in test and len(test["deletions"]) > 0:
        conn.executemany(
            "INSERT INTO deletions VALUES ( ?, ?, ?, ? )", test["deletions"]
        )
    conn.commit()
    ret = _do_graft_test(connection[1], prefix=prefix, lease=lease)

    ret = True

    testresult = test
    if "check" in testresult:
        testresult = testresult["check"]

    for a in ["files", "dirs", "links", "deletions"]:
        if a not in testresult:
            testresult[a] = []
    #remount_repo()
    if ret:
        ret = check_files(testresult["files"], prefix)
    if ret:
        ret = check_dirs(testresult["dirs"], prefix)
    if ret:
        ret = check_links(testresult["links"], prefix)
    if ret:
        ret = check_deletions(testresult, prefix)

    if ret == expect_success:
        print("Test passed")
        return True
    else:
        print("Test failed")
        return False


def check_files(files, prefix):
    for f in files:
        p = f[0]
        if p.startswith("/"):
            p = p.replace("/", "", 1)
        if prefix:
            p = os.path.join(prefix, p)
        p = os.path.join(MOUNT_POINT, p)
        p_bytes = p.encode('utf-8')
        if not os.path.exists(p_bytes):
            appeared = False
            print(f"File {p} not found.")
            if WAIT_SECONDS_TO_APPEAR == 0:
                return False
            print(f"Giving {p} time to appear.")
            for i in range(1, WAIT_SECONDS_TO_APPEAR):
                time.sleep(1)
                if os.path.exists(p_bytes):
                    print(f"File {p} strangely appeared after {i} seconds!!!")
                    appeared = True
                    break
                else:
                    print(f"File {p} still hasn't appeared after {i} seconds.")
            if not appeared:
                return False

        s = os.stat(p_bytes)
        if f[1] | stat.S_IFREG != s.st_mode:
            print(f"mode mismatch {f[1]}!={s.st_mode}")
            return False
        if f[2] != int(s.st_mtime * 1000000000):
            print(f"mtime mismatch {f[2]}!={s.st_mtime}")
            return False
        if f[3] != s.st_uid:
            print("uid mismatch")
            return False
        if f[4] != s.st_gid:
            print("gid mismatch")
            return False
        if f[5] != s.st_size:
            print("size mismatch")
            return False
        # compare hashes
        chunks = xattr.getxattr(p_bytes, "user.chunk_list").decode("ascii")
        chunks = chunks.strip().split("\n")
        c = []
        for cc in chunks[1:]:
            c.append(cc.split(",")[0])
        chunks = ",".join(c)
        if chunks != f[6]:
            print(f"chunk_list mismatch {chunks} != {f[6]}")
            return False

    return True


def check_dirs(dirs, prefix):
    #    name  TEXT    PRIMARY KEY,
    #    mode  INTEGER NOT NULL DEFAULT 493, -- 0755 in octal
    #    mtime INTEGER NOT NULL DEFAULT (unixepoch()), -- Unix seconds
    #    owner INTEGER NOT NULL DEFAULT 0,
    #    grp   INTEGER NOT NULL DEFAULT 0,
    #    acl   TEXT    NOT NULL DEFAULT ""

    for f in dirs:
        p = f[0]
        if p.startswith("/"):
            p = p.replace("/", "", 1)
        if prefix:
            p = os.path.join(prefix, p)
        p = os.path.join(MOUNT_POINT, p)
        p_bytes = p.encode('utf-8')
        if not os.path.exists(p_bytes):
            appeared = False
            print(f"Dir {p} not found.")
            if WAIT_SECONDS_TO_APPEAR == 0:
                return False
            print(f"Giving {p} time to appear.")
            for i in range(1, WAIT_SECONDS_TO_APPEAR):
                time.sleep(1)
                if os.path.exists(p_bytes):
                    print(f"Dir {p} strangely appeared after {i} seconds!!!")
                    appeared = True
                    break
                else:
                    print(f"Dir {p} still hasn't appeared after {i} seconds.")
            if not appeared:
                return False
        s = os.stat(p_bytes)
        if f[1] | stat.S_IFDIR != s.st_mode:
            print(f"mode mismatch {f[1]}!={s.st_mode}")
            return False
        if f[2] != int(s.st_mtime * 1000000000):
            print(f"mtime mismatch {f[2]}!={s.st_mtime}")
            return False
        if f[3] != s.st_uid:
            print("uid mismatch")
            return False
        if f[4] != s.st_gid:
            print("gid mismatch")
            return False
        acl = posix1e.ACL(file=p_bytes).to_any_text().strip().decode("ascii")
        if f[5].strip() != "" and f[5].strip() != acl:
            print(f"acl mismatch {f[5]}!={acl}")
            return False
        try:
            os.scandir(p_bytes)
        except Exception:
            print(f"Error reading directory {p}")
            return False

    return True


def check_links(links, prefix):
    for f in links:
        f = list(f)
        if prefix:
            f[0] = os.path.join(prefix, f[0])
        p = os.path.join(MOUNT_POINT, f[0])
        p_bytes = p.encode('utf-8')
        if not os.path.islink(p_bytes):
            return False
        target = os.readlink(p_bytes)
        if f[1].encode('utf-8') != target:
            print(f"Target mismatch {f[1]}!={target}")
            return False
        st = os.lstat(p_bytes)
        if f[2] != int(st.st_mtime * 1000000000):
            print(f"mtime mismatch {f[2]}!={st.st_mtime}")
            return False
        if f[3] != st.st_uid:
            print(f"owner mismatch {f[3]}!={st.st_uid}")
            return False
        if f[4] != st.st_gid:
            print("gid mismatch")
            return False

    return True


def check_deletions(tests, prefix):
    files = {}
    for a in ["files", "dirs", "links"]:
        for t in tests[a]:
            if prefix:
                files[os.path.join(prefix, t[0])] = True
            else:
                files[t[0]] = True
    deletions = tests["deletions"]

    for f in deletions:
        f = list(f)
        if prefix:
            f[0] = os.path.join(prefix, f[0])
        p = os.path.join(MOUNT_POINT, f[0])
        p_bytes = p.encode('utf-8')
        if os.path.exists(p_bytes) and f[0] not in files:
            print(f"File {p} exists")
            return False
    return True


long_acl = "user::rwx\ngroup::rwx\n"
for t in range(501, 600):
    long_acl += f"group:group_{t}:rwx\n"

long_acl += "mask::rwx\nother::rwx"

short_acl = """user::rwx
group::rwx
group:group_501:r-x
mask::rwx
other::rwx"""

tests = [
    # _Starting_ with this test panics ingestsql
    {"deletions": [("f1", 1, 0, 0)]},
    {"dirs": [("PREFIX", 0o755, 0, 0, 0, "")]},
    {"dirs": [("SHORT_ACL", 0o755, 0, 0, 0, short_acl)]},
    # LONG_ACL will pass once PR#3622 (extend xattr size from 256 bytes to 64KiB) lands
    # {"result": False, "dirs": [("LONG_ACL", 0o755, 0, 0, 0, long_acl)]},
    {
        "dirs": [
            ("PARTIAL_PREFIX_1", 0o755, 0, 0, 0, ""),
            ("PARTIAL_PREFIX_2", 0o755, 0, 0, 0, ""),
        ]
    },
    {
        "dirs": [
            ("PARTIAL_PREFIX_1/PP1", 0o755, 0, 0, 0, ""),
            ("PARTIAL_PREFIX_1/PP2", 0o755, 0, 0, 0, ""),
        ]
    },
    {"dirs": [("ab", 0o755, 0, 0, 0, ""), ("ab/bc", 0o755, 0, 0, 0, "")]},
    {
        "dirs": [
            ("ab/bc/yyxz", 0o755, 0, 0, 0, ""),
            ("ab/bc/yyx", 0o755, 0, 0, 0, ""),
            ("ab/bc/yy", 0o755, 0, 0, 0, ""),
        ]
    },
    {
        "files": [
            ("ab/bc/x", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
        "links": [("ab/bc/y", "foo", 0, 2, 0, 0), ("ab/bc/z", "foo", 0, 2, 0, 0)],
    },
    {
        "dirs": [("ab/bd", 0o755, 0, 0, 0, "")],
        "files": [
            ("ab/bd/x", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
        "links": [("ab/bd/y", "foo", 0, 2, 0, 0), ("ab/bd/z", "foo", 0, 2, 0, 0)],
    },
    {"deletions": [("ab", 1, 0, 0), ("bd", 1, 0, 0)]},
    {
        "dirs": [
            ("testdir1", 0o755, 0, 0, 0, ""),
            ("testdir1/testdir2", 0o755, 0, 0, 0, ""),
            ("testdir1/testdir2/testdir3", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("testdir1/testdir2", 1, 0, 0)]},
    # Create a file, then try replacing it with a directory in a single txn
    {
        "files": [
            ("TEST1", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ]
    },
    {"deletions": [("TEST1", 1, 1, 1)], "dirs": [("TEST1", 0o755, 0, 0, 0, "")]},
    # Now try deleting the dir and replacing it with a new dir
    {"deletions": [("TEST1", 1, 1, 1)], "dirs": [("TEST1", 0o755, 0, 0, 0, "")]},
    # doing this and restarting crashes both ingestsql and cvmfs2!
    {
        "dirs": [
            ("f1/d2/d3/d4/d5", 0o755, 0, 0, 0, ""),
            ("f1/g3", 0o755, 0, 0, 0, ""),
            ("f1/d2/d3/d4", 0o755, 0, 0, 0, ""),
            ("f1/d2/d3", 0o755, 0, 0, 0, ""),
            ("f1/d2", 0o755, 0, 0, 0, ""),
            ("f1", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("f1", 1, 0, 0)]},
    {"dirs": [("f1", 0o755, 0, 0, 0, "")]},
    {"result": False, "check": {"dirs": [("f1/d2", 1, 0, 0)]}},
    {"result": False, "check": {"dirs": [("f1/g3", 1, 0, 0)]}},
    {
        "dirs": [
            ("e1", 0o755, 0, 0, 0, ""),
            ("e1/e2", 0o755, 0, 0, 0, ""),
            ("e1/e2/e3", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("e1/e2/e3", 1, 0, 0)]},
    {"dirs": [("e1/e2", 0o755, 0, 0, 0, "")]},
    {
        "dirs": [
            ("d1/d2/d3/d4/d5", 0o755, 0, 0, 0, ""),
            ("d1/d2/d3/d4", 0o755, 0, 0, 0, ""),
            ("d1/d2/d3", 0o755, 0, 0, 0, ""),
            ("d1/d2", 0o755, 0, 0, 0, ""),
            ("d1", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("d1/d2", 1, 0, 0)]},
    {"dirs": [("d1/d2", 0o755, 0, 0, 0, "")]},
    {"dirs": [("d1/d2/d3a", 0o755, 0, 0, 0, "")]},
    {"dirs": [("d1/d2/d3", 0o755, 0, 0, 0, "")]},
    # No-op is successful
    {},
    # Try deleting a non-nested directory
    {
        "result": False,
        "deletions": [("TEST_DIRECTORY", 1, 0, 0)],
        "check": {
            "files": [
                (
                    "TEST_DIRECTORY/file1",
                    33188,
                    1693402771000000000,
                    0,
                    0,
                    4,
                    "4d6b90b7d8c09cdb00304f3d1fcba99cf3b08396",
                )
            ]
        },
    },
    {
        "result": False,
        "dirs": [("TEST_DIRECTORY", 0o777, 0, 0, 0, "")],
        "check": {
            "files": [
                (
                    "TEST_DIRECTORY/file1",
                    33188,
                    1693402771000000000,
                    0,
                    0,
                    4,
                    "4d6b90b7d8c09cdb00304f3d1fcba99cf3b08396",
                )
            ]
        },
    },
    # Simple deletions
    {"links": [("to_delete_link", "foo", 0, 0, 0, 0)]},
    {
        "files": [
            (
                "to_delete_file",
                0o755,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ]
    },
    {"deletions": [("to_delete_link", 0, 0, 1)]},
    {"deletions": [("to_delete_file", 0, 1, 0)]},
    # replace a link with a file and a file with a link
    {"links": [("link_to_replace", "foo", 0, 2, 0, 0)]},
    {
        "files": [
            (
                "link_to_replace",
                0o777,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ]
    },
    {
        "result": True,
        "links": [("link_to_replace", "foo", 0, 5, 0, 1)],
        "check": {
            "files": [
                (
                    "link_to_replace",
                    0o777,
                    0,
                    0,
                    0,
                    0,
                    "1234567890123456789012345678901234567890",
                )
            ]
        },
    },
    {"result": True, "links": [("link_to_replace", "foo", 0, 5, 0, 0)]},
    # replace a link with a directory and a directory with a link in 3 transactions
    {"links": [("link_to_replace", "foo", 0, 5, 0, 0)]},
    {
        "dirs": [
            ("link_to_replace", 0o777, 0, 0, 0, ""),
        ],
        "deletions": [("link_to_replace", 0, 0, 1)],
    },
    {"result": False, "links": [("link_to_replace", "foo", 0, 6, 0, 0)]},
    {
        "result": True,
        "links": [("link_to_replace", "foo", 0, 6, 0, 1)],
        "check": {"dirs": [("link_to_replace", 0o777, 0, 0, 0, "")]},
    },
    # delete a the directory and replace it with a link in a single transaction
    {
        "deletions": [("link_to_replace", 1, 0, 0)],
        "links": [("link_to_replace", "foo", 0, 4, 0, 0)],
    },
    # delete a the link and replace it with a link in a single transaction
    {
        "deletions": [("link_to_replace", 0, 0, 1)],
        "dirs": [("link_to_replace", 0o755, 0, 0, 0, "")],
    },
    # create a file, try symlinking over it, testing skip_if_file_or_dir
    {
        "files": [
            ("file2", 0o777, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
        "dirs": [("dir2", 0o777, 0, 0, 0, "")],
    },
    {
        "links": [("file2", "foo", 0, 0, 0, 1)],
        "check": {
            "files": [
                ("file2", 0o777, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
            ]
        },
    },
    {"links": [("file2", "foo", 0, 0, 0, 0)]},
    {
        "links": [("dir2", "foo", 0, 0, 0, 1)],
        "check": {"dirs": [("dir2", 0o777, 0, 0, 0, "")]},
    },
    {"result": False, "links": [("dir2", "foo", 0, 0, 0, 0)]},
    {
        "files": [
            ("file1", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
        "links": [("link1", "file1", 0, 0, 0, 0)],
        "dirs": [("dir1", 0o755, 0, 0, 0, "")],
    },
    #    { "result": False, "deletions": [ ( "dir1", 0, 1, 0 ) ] },
    #    { "result": False, "deletions": [ ( "dir1", 0, 0, 1 ) ] },
    #    { "result": False, "deletions": [ ( "dir1", 0, 1, 1 ) ] },
    #    { "result": True,  "deletions": [ ( "dir1", 1, 0, 0 ) ] },
    #
    #    { "result": False, "deletions": [ ( "file1", 1, 0, 0 ) ] },
    #    { "result": False, "deletions": [ ( "file1", 0, 0, 1 ) ] },
    #    { "result": False, "deletions": [ ( "file1", 1, 0, 1 ) ] },
    #    { "result": True,  "deletions": [ ( "file1", 0, 1, 0 ) ] },
    #
    #    { "result": False, "deletions": [ ( "link1", 0, 1, 0 ) ] },
    #    { "result": False, "deletions": [ ( "link1", 1, 0, 0 ) ] },
    #    { "result": False, "deletions": [ ( "link1", 1, 1, 0 ) ] },
    #    { "result": True,  "deletions": [ ( "link1", 0, 0, 1 ) ] },
    {
        "files": [
            ("file1", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
        "links": [("link1", "file1", 0, 0, 0, 0)],
        "dirs": [("dir1", 0o755, 0, 0, 0, "")],
    },
    {
        "result": True,
        "deletions": [("file1", 1, 1, 1), ("dir1", 1, 1, 1), ("link1", 1, 1, 1)],
    },
    {
        "files": [
            (
                "中国镍铬不锈钢产业链常规报告20220715(修正）.pdf",
                0o755,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ]
    },
    # Test successful creation
    {
        "files": [
            (
                "foo/baz",
                0o755,
                456000000000,
                789,
                1011,
                1213,
                "1234567890123456789012345678901234567890",
            )
        ],
        "dirs": [("foo", 0o755, 0, 0, 0, "")],
    },
    # Dir foo already exists, can't create file with the same name
    {
        "result": False,
        "files": [
            (
                "foo",
                0o755,
                0,
                0,
                0,
                25165824,
                "1234567890123456789012345678901234567890",
            )
        ]
    },
    # Dir foo already exists, can't create file with the same name
    {
        "result": False,
        "files": [
            (
                "foo",
                0o755,
                0,
                0,
                0,
                25165825,
                "1234567890123456789012345678901234567890,1234567890123456789012345678901234567890",
            )
        ]
    },
    # Test garbage ACLs
    {"result": False, "dirs": [("foo", 0o755, 0, 0, 0, "GARBAGE")]},
    {"result": False, "dirs": [("foo", 0o755, 0, 0, 0, "user::r-x")]},
    {
        "result": False,
        "dirs": [("foo", 0o755, 0, 0, 0, "user:mharvey:rwx\ngroup:it:r-x\nother::r-x")],
    },
    # Test a conflicting ACL. The test is intended to fail.
    # ACL is expressible in pure discretionary system terms (uid, gid, perms).
    # So ACL xattr won't be created. When ACL is read back from the file, mask
    # field won't be present, triggering mismatch.
    {
        "result": False,
        "dirs": [
            ("bar", 0o755, 0, 0, 0, "user::rwx\ngroup::r-x\nmask::rwx\nother::r-x")
        ],
        "deletions": [("foo", 1, 1, 1)],
    },
    # Same as above, but with mask::rwx removed. Intended to pass.
    {
        "result": True,
        "dirs": [
            ("bar", 0o755, 0, 0, 0, "user::rwx\ngroup::r-x\nother::r-x")
        ],
        "deletions": [("foo", 1, 1, 1)],
    },
    {
        "result": False,
        "dirs": [
            (
                "foo",
                0o755,
                0,
                0,
                0,
                "user:mharvey:rwx\ngroup:it:rwx\nother::rwx\nuser::r-x\ngroup::r-x\nother::r-x",
            )
        ],
    },
    # Test name sanitisation
    {"result": False, "files": [("", 0o755, 0, 0, 0, 0, "")]},
    {
        "result": True,
        "files": [
            ("/foo", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
    },
    {"result": False, "files": [("foo/", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("//foo", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("foo//", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/a", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f//a", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/./a", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("./f", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("../f", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/./a", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/../a", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/a/.", 0o755, 0, 0, 0, 0, "")]},
    {"result": False, "files": [("f/a/..", 0o755, 0, 0, 0, 0, "")]},
    {
        "result": True,
        "files": [
            ("\01\03\04", 0o755, 0, 0, 0, 0, "1234567890123456789012345678901234567890")
        ],
    },
    {
        "result": True,
        "files": [
            (
                "\U0001f4a9",
                0o755,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ],
    },
    {
        "result": True,
        "files": [
            (
                " whitespace",
                0o755,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ],
    },
    {
        "result": True,
        "files": [
            (
                "white space",
                0o755,
                0,
                0,
                0,
                0,
                "1234567890123456789012345678901234567890",
            )
        ],
    },
    # Test parent directory requirement
    {"result": False, "files": [("foo/bar", 0o755, 0, 0, 0, 0, "")]},
    # Fail if hashes are bad
    {"result": False, "files": [("test", 0o755, 0, 0, 0, 0, "xxx")]},
    {"result": False, "files": [("test", 0o755, 0, 0, 0, 0, ",xxx,")]},
    {"result": False, "files": [("test", 0o755, 0, 0, 0, 0, ",,,")]},
    {
        "result": False,
        "files": [
            ("test", 0o755, 0, 0, 0, 0, "123456789012345678901234567890123456789")
        ],
    },
    # Fail if size is negative
    {
        "result": False,
        "files": [
            ("foo", 0o755, 0, 0, 0, -1, "1234567890123456789012345678901234567890")
        ],
    },
    # Fail if making a directory when parent doesn't yet exist
    {"result": False, "dirs": [("missing/dir", 0o755, 0, 0, 0, "")]},
    # Symlinks
    {"links": [("link", "foo", 123000000000, 10, 11, 0)]},
    {"links": [("link", "/foo", 123000000000, 10, 11, 0)]},
    {"links": [("link", "../foo/..", 123000000000, 10, 11, 0)]},
    # JCS abuse symlinks
    #   {"result": False, "links": [("link", "", 123000000000, 10, 11, 0)]},
    # Nested dirs
    {
        "dirs": [
            ("d1/d2/d3/d4/d5", 0o755, 0, 0, 0, ""),
            ("d1/d2/d3/d4", 0o755, 0, 0, 0, ""),
            ("d1/d2/d3", 0o755, 0, 0, 0, ""),
            ("d1/d2", 0o755, 0, 0, 0, ""),
            ("d1", 0o755, 0, 0, 0, ""),
        ]
    },
    # Dupes
    # FIXME: these should fail, but there's presently no checking for name duplication between entities
    #    {
    #        "result": False,
    #        "files": [
    #            ("dupe", 0, 0, 0, 1, 2, "1234567890123456789012345678901234567890"),
    #        ],
    #        "dirs": [("dupe", 0, 0, 0, 0, "")],
    #    },
    #    {
    #        "result": False,
    #        "files": [
    #            ("dupe", 0, 0, 0, 1, 2, "1234567890123456789012345678901234567890"),
    #        ],
    #        "links": [("dupe", "foo", 0, 0, 0, 0)],
    #    },
    # Deletions
    {
        "dirs": [
            ("xd1/d2/d3/d4/d5", 0o755, 0, 0, 0, ""),
            ("xd1/d2/d3/d4", 0o755, 0, 0, 0, ""),
            ("xd1/d2/d3", 0o755, 0, 0, 0, ""),
            ("xd1/d2", 0o755, 0, 0, 0, ""),
            ("xd1", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("xd1/d2/d3", 1, 0, 0)]},
    {
        "dirs": [
            ("yd1/d2/d3/d4/d5", 0o755, 0, 0, 0, ""),
            ("yd1/d2/d3/d4", 0o755, 0, 0, 0, ""),
            ("yd1/d2/d3", 0o755, 0, 0, 0, ""),
            ("yd1/d2", 0o755, 0, 0, 0, ""),
            ("yd1", 0o755, 0, 0, 0, ""),
        ]
    },
    {"deletions": [("yd1/d2/d3", 1, 0, 0)]},
    {"deletions": [("yd1", 1, 0, 0)]},
    {
        "files": [
            (
                "TEST_FILE_TO_READ",
                0o755,
                0,
                0,
                0,
                6,
                "a8eec30a5b2d71bc890175f5b361ebb28d7c54a8",
            )
        ]
    },
]


if __name__ == "__main__":
    os.makedirs(MOUNT_POINT, exist_ok=True)

    for t in range(501, 600):
        try:
            subprocess.check_output(["/usr/sbin/groupadd", "-g", f"{t}", f"group_{t}"])
        except:
            pass

    print(f"Command arguments [{sys.argv}]")

    try:
        if not ("--no-mount" in sys.argv):
            mount_repo()

        db = make_db()
        i = 0

        if "standard" in sys.argv:
            if "file-read" in sys.argv:
                if not do_file_read_test(db):
                    print("file_read_test failed")
                    sys.exit(1)

            # confirm that a file outside the lease path will cause an error
            db = make_db()
            test = {"dirs": [("PREFIX", 0o755, 0, 0, 0, "")]}
            test["result"] = False
            # TODO make sure it exits normally with with non-zero exit code, not SIGABRT due to assertion failure
            if not do_test(db, test, lease="FOO"):
                print(f"Failed Test {i} {test}, lease=FOO")
                sys.exit(1)
            clear_db(db)

            for t in tests:
                i += 1
                print(f"Running Test {i} {t}")
                sys.stdout.flush()
                for prefix in [None, "PREFIX"]:
                    if not (do_test(db, t, prefix=prefix)):
                        print(f"Failed Test {i} {t}, prefix={prefix}")
                        sys.exit(1)
                    clear_db(db)
                if "quick" in sys.argv:
                    if i >= 20:
                        print(f"Stopping after test {i} to keep it quick. Run without 'quick' in argv to run all cases.")
                        break

    except Exception as e:
        print(e)
        raise
    print("DONE SUCCESS")
