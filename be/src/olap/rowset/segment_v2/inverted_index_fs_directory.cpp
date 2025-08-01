// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"

#include "CLucene/SharedHeader.h"
#include "CLucene/_SharedHeader.h"
#include "common/status.h"
#include "inverted_index_common.h"
#include "inverted_index_desc.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "olap/tablet_schema.h"
#include "util/debug_points.h"
#include "util/slice.h"

#ifdef _CL_HAVE_IO_H
#include <io.h>
#endif
#ifdef _CL_HAVE_SYS_STAT_H
#include <sys/stat.h>
#endif
#ifdef _CL_HAVE_UNISTD_H
#include <unistd.h>
#endif
#ifdef _CL_HAVE_DIRECT_H
#include <direct.h>
#endif
#include <CLucene/LuceneThreads.h>
#include <CLucene/clucene-config.h>
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/index/IndexWriter.h>
#include <CLucene/store/LockFactory.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <glog/logging.h>
#include <stdio.h>
#include <string.h>
#include <wchar.h>

#include <filesystem>
#include <iostream>
#include <mutex>
#include <utility>

#define CL_MAX_PATH 4096
#define CL_MAX_DIR CL_MAX_PATH

#if defined(_WIN32) || defined(_WIN64)
#define PATH_DELIMITERA "\\"
#else
#define PATH_DELIMITERA "/"
#endif

#define LOG_AND_THROW_IF_ERROR(status, msg)                                  \
    do {                                                                     \
        auto _status_result = (status);                                      \
        if (!_status_result.ok()) {                                          \
            auto err = std::string(msg) + ": " + _status_result.to_string(); \
            LOG(WARNING) << err;                                             \
            _CLTHROWA(CL_ERR_IO, err.c_str());                               \
        }                                                                    \
    } while (0)

namespace doris::segment_v2 {

const char* const DorisFSDirectory::WRITE_LOCK_FILE = "write.lock";

bool DorisFSDirectory::FSIndexInput::open(const io::FileSystemSPtr& fs, const char* path,
                                          IndexInput*& ret, CLuceneError& error,
                                          int32_t buffer_size, int64_t file_size) {
    CND_PRECONDITION(path != nullptr, "path is NULL");

    if (buffer_size == -1) {
        buffer_size = CL_NS(store)::BufferedIndexOutput::BUFFER_SIZE;
    }
    auto h = std::make_shared<SharedHandle>(path);

    io::FileReaderOptions reader_options;
    reader_options.cache_type = config::enable_file_cache ? io::FileCachePolicy::FILE_BLOCK_CACHE
                                                          : io::FileCachePolicy::NO_CACHE;
    reader_options.is_doris_table = true;
    reader_options.file_size = file_size;
    Status st = fs->open_file(path, &h->_reader, &reader_options);
    DBUG_EXECUTE_IF("inverted file read error: index file not found",
                    { st = Status::Error<doris::ErrorCode::NOT_FOUND>("index file not found"); })
    if (st.code() == ErrorCode::NOT_FOUND) {
        error.set(CL_ERR_FileNotFound, fmt::format("File does not exist, file is {}", path).data());
    } else if (st.code() == ErrorCode::IO_ERROR) {
        error.set(CL_ERR_IO, fmt::format("File open io error, file is {}", path).data());
    } else if (st.code() == ErrorCode::PERMISSION_DENIED) {
        error.set(CL_ERR_IO, fmt::format("File Access denied, file is {}", path).data());
    } else if (!st.ok()) {
        error.set(CL_ERR_IO, fmt::format("Could not open file, file is {}", path).data());
    }

    //Check if a valid handle was retrieved
    if (st.ok() && h->_reader) {
        if (h->_reader->size() == 0) {
            // may be an empty file
            LOG(INFO) << "Opened inverted index file is empty, file is " << path;
            // need to return false to avoid the error of CLucene
            error.set(CL_ERR_EmptyIndexSegment,
                      fmt::format("File is empty, file is {}", path).data());
            return false;
        }
        //Store the file length
        h->_length = h->_reader->size();
        h->_fpos = 0;
        ret = _CLNEW FSIndexInput(std::move(h), buffer_size);
        return true;
    }

    //delete h->_shared_lock;
    //_CLDECDELETE(h)
    return false;
}

DorisFSDirectory::FSIndexInput::FSIndexInput(const FSIndexInput& other)
        : BufferedIndexInput(other) {
    if (other._handle == nullptr) {
        _CLTHROWA(CL_ERR_NullPointer, "other handle is null");
    }

    std::lock_guard<std::mutex> wlock(other._handle->_shared_lock);
    _handle = other._handle;
    _pos = other._handle->_fpos; //note where we are currently...
    _io_ctx = other._io_ctx;
}

DorisFSDirectory::FSIndexInput::SharedHandle::SharedHandle(const char* path) {
    _length = 0;
    _fpos = 0;
    strcpy(this->path, path);
    //_shared_lock = new std::mutex();
}

DorisFSDirectory::FSIndexInput::SharedHandle::~SharedHandle() {
    if (_reader) {
        auto st = _reader->close();
        DBUG_EXECUTE_IF("FSIndexInput::~SharedHandle_reader_close_error",
                        { st = Status::Error<doris::ErrorCode::NOT_FOUND>("failed to close"); });
        if (st.ok()) {
            _reader = nullptr;
        }
    }
}

DorisFSDirectory::FSIndexInput::~FSIndexInput() {
    FSIndexInput::close();
}

lucene::store::IndexInput* DorisFSDirectory::FSIndexInput::clone() const {
    return _CLNEW DorisFSDirectory::FSIndexInput(*this);
}
void DorisFSDirectory::FSIndexInput::close() {
    BufferedIndexInput::close();
}

void DorisFSDirectory::FSIndexInput::setIoContext(const void* io_ctx) {
    if (io_ctx) {
        const auto& ctx = static_cast<const io::IOContext*>(io_ctx);
        _io_ctx.reader_type = ctx->reader_type;
        _io_ctx.query_id = ctx->query_id;
        _io_ctx.file_cache_stats = ctx->file_cache_stats;
    } else {
        _io_ctx.reader_type = ReaderType::UNKNOWN;
        _io_ctx.query_id = nullptr;
        _io_ctx.file_cache_stats = nullptr;
    }
}

const void* DorisFSDirectory::FSIndexInput::getIoContext() {
    return &_io_ctx;
}

void DorisFSDirectory::FSIndexInput::setIndexFile(bool isIndexFile) {
    _io_ctx.is_index_data = isIndexFile;
}

void DorisFSDirectory::FSIndexInput::seekInternal(const int64_t position) {
    CND_PRECONDITION(position >= 0 && position < _handle->_length, "Seeking out of range");
    _pos = position;
}

/** IndexInput methods */
void DorisFSDirectory::FSIndexInput::readInternal(uint8_t* b, const int32_t len) {
    CND_PRECONDITION(_handle != nullptr, "shared file handle has closed");
    CND_PRECONDITION(_handle->_reader != nullptr, "file is not open");

    int64_t inverted_index_io_timer = 0;
    {
        SCOPED_RAW_TIMER(&inverted_index_io_timer);

        std::lock_guard<std::mutex> wlock(_handle->_shared_lock);

        int64_t position = getFilePointer();
        if (_pos != position) {
            _pos = position;
        }

        if (_handle->_fpos != _pos) {
            _handle->_fpos = _pos;
        }

        Slice result {b, (size_t)len};
        size_t bytes_read = 0;
        Status st = _handle->_reader->read_at(_pos, result, &bytes_read, &_io_ctx);
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error", {
            st = Status::InternalError(
                    "debug point: "
                    "DorisFSDirectory::FSIndexInput::readInternal_reader_read_at_error");
        })
        if (!st.ok()) {
            _CLTHROWA(CL_ERR_IO, "read past EOF");
        }
        bufferLength = len;
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexInput::readInternal_bytes_read_error",
                        { bytes_read = len + 10; })
        if (bytes_read != len) {
            _CLTHROWA(CL_ERR_IO, "read error");
        }
        _pos += bufferLength;
        _handle->_fpos = _pos;
    }

    if (_io_ctx.file_cache_stats != nullptr) {
        _io_ctx.file_cache_stats->inverted_index_io_timer += inverted_index_io_timer;
    }
}

void DorisFSDirectory::FSIndexOutput::init(const io::FileSystemSPtr& fs, const char* path) {
    DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput::init.file_cache", {
        if (fs->type() == io::FileSystemType::S3 && _opts.write_file_cache == false) {
            _CLTHROWA(CL_ERR_IO, "Inverted index failed to enter file cache");
        }
    });

    Status status = fs->create_file(path, &_writer, &_opts);
    DBUG_EXECUTE_IF(
            "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_"
            "init",
            {
                status = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                        "debug point: test throw error in fsindexoutput init mock error");
            })
    if (!status.ok()) {
        _writer.reset(nullptr);
        auto err = "Create compound file error: " + status.to_string();
        LOG(WARNING) << err;
        _CLTHROWA(CL_ERR_IO, err.c_str());
    }
}

DorisFSDirectory::FSIndexOutput::~FSIndexOutput() {
    if (_writer) {
        try {
            FSIndexOutput::close();
            DBUG_EXECUTE_IF(
                    "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_"
                    "destructor",
                    {
                        _CLTHROWA(CL_ERR_IO,
                                  "debug point: test throw error in fsindexoutput destructor");
                    })
        } catch (CLuceneError& err) {
            //ignore errors...
            LOG(WARNING) << "FSIndexOutput deconstruct error: " << err.what();
        }
    }
}

void DorisFSDirectory::FSIndexOutput::flushBuffer(const uint8_t* b, const int32_t size) {
    if (_writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_"
                "flushBuffer",
                {
                    if (_writer->path().filename() == "_0.tii" ||
                        _writer->path().filename() == "_0.tis") {
                        return;
                    }
                })
        Status st = _writer->append(data);
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer", {
                    st = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "flush buffer mock error");
                })
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
            _CLTHROWA(CL_ERR_IO, "writer append data when flushBuffer error");
        }
    } else {
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput::flushBuffer_writer_is_nullptr",
                        { _writer = nullptr; })
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput::flushBuffer_b_is_nullptr",
                        { b = nullptr; })
        if (_writer == nullptr) {
            LOG(WARNING) << "File writer is nullptr in DorisFSDirectory::FSIndexOutput, "
                            "ignore flush.";
        } else if (b == nullptr) {
            LOG(WARNING) << "buffer is nullptr when flushBuffer in "
                            "DorisFSDirectory::FSIndexOutput";
        }
    }
}

void DorisFSDirectory::FSIndexOutput::close() {
    try {
        BufferedIndexOutput::close();
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_close",
                {
                    _CLTHROWA(CL_ERR_IO,
                              "debug point: test throw error in bufferedindexoutput close");
                })
    } catch (CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close error: " << err.what();
        if (err.number() == CL_ERR_IO) {
            LOG(WARNING) << "FSIndexOutput close, BufferedIndexOutput close IO error: "
                         << err.what();
        }
        _writer.reset(nullptr);
        _CLTHROWA(err.number(), err.what());
    }
    DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput.set_writer_nullptr", {
        LOG(WARNING) << "Dbug execute, set _writer to nullptr";
        _writer = nullptr;
    })
    if (_writer) {
        auto ret = _writer->close();
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error",
                        { ret = Status::Error<INTERNAL_ERROR>("writer close status error"); })
        if (!ret.ok()) {
            LOG(WARNING) << "FSIndexOutput close, file writer close error: " << ret.to_string();
            _writer.reset(nullptr);
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
        _CLTHROWA(CL_ERR_IO, "close file writer error, _writer = nullptr");
    }
    _writer.reset(nullptr);
}

int64_t DorisFSDirectory::FSIndexOutput::length() const {
    CND_PRECONDITION(_writer != nullptr, "file is not open");
    return _writer->bytes_appended();
}

void DorisFSDirectory::FSIndexOutputV2::init(io::FileWriter* file_writer) {
    _index_v2_file_writer = file_writer;
    DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_fsindexoutput_init", {
        _CLTHROWA(CL_ERR_IO, "debug point: test throw error in fsindexoutput init mock error");
    })
}

DorisFSDirectory::FSIndexOutputV2::~FSIndexOutputV2() {}

void DorisFSDirectory::FSIndexOutputV2::flushBuffer(const uint8_t* b, const int32_t size) {
    if (_index_v2_file_writer != nullptr && b != nullptr && size > 0) {
        Slice data {b, (size_t)size};
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._mock_append_data_error_in_fsindexoutput_"
                "flushBuffer",
                { return; })
        Status st = _index_v2_file_writer->append(data);
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._status_error_in_fsindexoutput_flushBuffer", {
                    st = Status::Error<doris::ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "flush buffer mock error");
                })
        if (!st.ok()) {
            LOG(WARNING) << "File IO Write error: " << st.to_string();
            _CLTHROWA(CL_ERR_IO, "writer append data when flushBuffer error");
        }
    } else {
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutputV2::flushBuffer_file_writer_is_nullptr",
                        { _index_v2_file_writer = nullptr; })
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutputV2::flushBuffer_b_is_nullptr",
                        { b = nullptr; })
        if (_index_v2_file_writer == nullptr) {
            LOG(WARNING) << "File writer is nullptr in DorisFSDirectory::FSIndexOutputV2, "
                            "ignore flush.";
            _CLTHROWA(CL_ERR_IO, "flushBuffer error, _index_v2_file_writer = nullptr");
        } else if (b == nullptr) {
            LOG(WARNING) << "buffer is nullptr when flushBuffer in "
                            "DorisFSDirectory::FSIndexOutput";
        }
    }
}

void DorisFSDirectory::FSIndexOutputV2::close() {
    try {
        BufferedIndexOutput::close();
        DBUG_EXECUTE_IF(
                "DorisFSDirectory::FSIndexOutput._throw_clucene_error_in_bufferedindexoutput_close",
                {
                    _CLTHROWA(CL_ERR_IO,
                              "debug point: test throw error in bufferedindexoutput close");
                })
    } catch (CLuceneError& err) {
        LOG(WARNING) << "FSIndexOutputV2 close, BufferedIndexOutput close error: " << err.what();
        if (err.number() == CL_ERR_IO) {
            LOG(WARNING) << "FSIndexOutputV2 close, BufferedIndexOutput close IO error: "
                         << err.what();
        }
        _CLTHROWA(err.number(), err.what());
    }
    DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput.set_writer_nullptr", {
        LOG(WARNING) << "Dbug execute, set _index_v2_file_writer to nullptr";
        _index_v2_file_writer = nullptr;
    })
    if (_index_v2_file_writer) {
        auto ret = _index_v2_file_writer->close();
        DBUG_EXECUTE_IF("DorisFSDirectory::FSIndexOutput._set_writer_close_status_error",
                        { ret = Status::Error<INTERNAL_ERROR>("writer close status error"); })
        if (!ret.ok()) {
            LOG(WARNING) << "FSIndexOutputV2 close, stream sink file writer close error: "
                         << ret.to_string();
            _CLTHROWA(CL_ERR_IO, ret.to_string().c_str());
        }
    } else {
        LOG(WARNING) << "File writer is nullptr, ignore finalize and close.";
        _CLTHROWA(CL_ERR_IO, "close file writer error, _index_v2_file_writer = nullptr");
    }
}

int64_t DorisFSDirectory::FSIndexOutputV2::length() const {
    if (_index_v2_file_writer == nullptr) {
        _CLTHROWA(CL_ERR_IO, "file is not open, index_v2_file_writer is nullptr");
    }
    return _index_v2_file_writer->bytes_appended();
}

DorisFSDirectory::DorisFSDirectory() {
    filemode = 0644;
    this->lockFactory = nullptr;
}

void DorisFSDirectory::init(const io::FileSystemSPtr& fs, const char* path,
                            lucene::store::LockFactory* lock_factory) {
    _fs = fs;
    directory = path;
    if (lock_factory == nullptr) {
        lock_factory = _CLNEW lucene::store::NoLockFactory();
    }

    lucene::store::Directory::setLockFactory(lock_factory);
}

std::string DorisFSDirectory::priv_getFN(const std::string& name) const {
    doris::io::Path path(directory);
    path /= name;
    return path.native();
}

DorisFSDirectory::~DorisFSDirectory() = default;

const char* DorisFSDirectory::getClassName() {
    return "DorisFSDirectory";
}
const char* DorisFSDirectory::getObjectName() const {
    return getClassName();
}

bool DorisFSDirectory::list(std::vector<std::string>* names) const {
    CND_PRECONDITION(!directory.empty(), "directory is not open");
    std::string fl = priv_getFN("");
    std::vector<io::FileInfo> files;
    bool exists;
    auto st = _fs->list(fl, true, &files, &exists);
    DBUG_EXECUTE_IF("DorisFSDirectory::list_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::list_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "List file IO error");
    DBUG_EXECUTE_IF("DorisFSDirectory::list_directory_not_exists", { exists = false; })
    if (!exists) {
        LOG_AND_THROW_IF_ERROR(st, fmt::format("Directory {} is not exist", fl));
    }
    for (auto& file : files) {
        names->push_back(file.file_name);
    }
    return true;
}

bool DorisFSDirectory::fileExists(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string fl = priv_getFN(name);
    bool exists = false;
    auto st = _fs->exists(fl, &exists);
    DBUG_EXECUTE_IF("DorisFSDirectory::fileExists_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::fileExists_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "File exists IO error");
    return exists;
}

const std::string& DorisFSDirectory::getDirName() const {
    return directory;
}

int64_t DorisFSDirectory::fileModified(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    struct stat buf;
    std::string buffer = priv_getFN(name);
    if (stat(buffer.c_str(), &buf) == -1) {
        return 0;
    } else {
        return buf.st_mtime;
    }
}

void DorisFSDirectory::touchFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    char buffer[CL_MAX_DIR];
    snprintf(buffer, CL_MAX_DIR, "%s%s%s", directory.c_str(), PATH_DELIMITERA, name);

    io::FileWriterPtr tmp_writer;
    auto st = _fs->create_file(buffer, &tmp_writer);
    DBUG_EXECUTE_IF("DorisFSDirectory::touchFile_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::touchFile_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "Touch file IO error");
}

int64_t DorisFSDirectory::fileLength(const char* name) const {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string buffer = priv_getFN(name);
    int64_t size = -1;
    Status st = _fs->file_size(buffer, &size);
    DBUG_EXECUTE_IF("inverted file read error: index file not found",
                    { st = Status::Error<doris::ErrorCode::NOT_FOUND>("index file not found"); })
    if (st.code() == ErrorCode::NOT_FOUND) {
        _CLTHROWA(CL_ERR_FileNotFound, "File does not exist");
    }
    DBUG_EXECUTE_IF("DorisFSDirectory::fileLength_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::fileLength_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "Get file size IO error");
    return size;
}

bool DorisFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                 CLuceneError& error, int32_t bufferSize) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string fl = priv_getFN(name);
    return FSIndexInput::open(_fs, fl.c_str(), ret, error, bufferSize);
}

void DorisFSDirectory::close() {
    DBUG_EXECUTE_IF("DorisFSDirectory::close_close_with_error",
                    { _CLTHROWA(CL_ERR_IO, "debug_point: close DorisFSDirectory error"); })
}

bool DorisFSDirectory::doDeleteFile(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string fl = priv_getFN(name);
    auto st = _fs->delete_file(fl);
    DBUG_EXECUTE_IF("DorisFSDirectory::doDeleteFile_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::doDeleteFile_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "Delete file IO error");
    return true;
}

bool DorisFSDirectory::deleteDirectory() {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string fl = priv_getFN("");
    auto st = _fs->delete_directory(fl);
    DBUG_EXECUTE_IF("DorisFSDirectory::deleteDirectory_throw_is_not_directory", {
        st = Status::Error<ErrorCode::NOT_FOUND>(
                fmt::format("debug point: {} is not a directory", fl));
    })
    LOG_AND_THROW_IF_ERROR(st, fmt::format("Delete directory {} IO error", fl));
    return true;
}

void DorisFSDirectory::renameFile(const char* from, const char* to) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::lock_guard<std::mutex> wlock(_this_lock);

    std::string old = priv_getFN(from);
    std::string nu = priv_getFN(to);

    bool exists = false;
    auto st = _fs->exists(nu, &exists);
    DBUG_EXECUTE_IF("DorisFSDirectory::renameFile_exists_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::renameFile_exists_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "File exists IO error");
    if (exists) {
        st = _fs->delete_directory(nu);
        DBUG_EXECUTE_IF("DorisFSDirectory::renameFile_delete_status_is_not_ok", {
            st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "debug point: DorisFSDirectory::renameFile_delete_status_is_not_ok");
        })
        LOG_AND_THROW_IF_ERROR(st, fmt::format("Delete {} IO error", nu));
    }
    st = _fs->rename(old, nu);
    DBUG_EXECUTE_IF("DorisFSDirectory::renameFile_rename_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::renameFile_rename_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, fmt::format("Rename {} to {} IO error", old, nu));
}

lucene::store::IndexOutput* DorisFSDirectory::createOutput(const char* name) {
    CND_PRECONDITION(directory[0] != 0, "directory is not open");
    std::string fl = priv_getFN(name);
    bool exists = false;
    auto st = _fs->exists(fl, &exists);
    DBUG_EXECUTE_IF("DorisFSDirectory::createOutput_exists_status_is_not_ok", {
        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                "debug point: DorisFSDirectory::createOutput_exists_status_is_not_ok");
    })
    LOG_AND_THROW_IF_ERROR(st, "Create output file exists IO error");
    if (exists) {
        st = _fs->delete_file(fl);
        DBUG_EXECUTE_IF("DorisFSDirectory::createOutput_delete_status_is_not_ok", {
            st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "debug point: DorisFSDirectory::createOutput_delete_status_is_not_ok");
        })
        LOG_AND_THROW_IF_ERROR(st, fmt::format("Create output delete file {} IO error", fl));
        st = _fs->exists(fl, &exists);
        DBUG_EXECUTE_IF("DorisFSDirectory::createOutput_exists_after_delete_status_is_not_ok", {
            st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "debug point: "
                    "DorisFSDirectory::createOutput_exists_after_delete_status_is_not_ok");
        })
        LOG_AND_THROW_IF_ERROR(st, "Create output file exists IO error");
        DBUG_EXECUTE_IF("DorisFSDirectory::createOutput_exists_after_delete_error",
                        { exists = true; })
        if (exists) {
            _CLTHROWA(CL_ERR_IO, fmt::format("File {} should not exist", fl).c_str());
        }
        assert(!exists);
    }
    auto* ret = _CLNEW FSIndexOutput();
    ErrorContext error_context;
    ret->set_file_writer_opts(_opts);
    try {
        ret->init(_fs, fl.c_str());
    } catch (CLuceneError& err) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("FSIndexOutput init error: ");
        error_context.err_msg.append(err.what());
        LOG(ERROR) << error_context.err_msg;
    }
    FINALLY_EXCEPTION({
        if (error_context.eptr) {
            FINALLY_CLOSE(ret);
            _CLDELETE(ret);
        }
    })
    return ret;
}

std::unique_ptr<lucene::store::IndexOutput> DorisFSDirectory::createOutputV2(
        io::FileWriter* file_writer) {
    auto ret = std::make_unique<FSIndexOutputV2>();
    ErrorContext error_context;
    try {
        ret->init(file_writer);
    } catch (CLuceneError& err) {
        error_context.eptr = std::current_exception();
        error_context.err_msg.append("FSIndexOutputV2 init error: ");
        error_context.err_msg.append(err.what());
        LOG(ERROR) << error_context.err_msg;
    }
    FINALLY_EXCEPTION({
        if (error_context.eptr) {
            FINALLY_CLOSE(ret);
        }
    })
    return ret;
}

std::string DorisFSDirectory::toString() const {
    return std::string("DorisFSDirectory@") + this->directory;
}

DorisRAMFSDirectory::DorisRAMFSDirectory() {
    filesMap = _CLNEW FileMap(true, true);
    this->sizeInBytes = 0;
}

DorisRAMFSDirectory::~DorisRAMFSDirectory() {
    std::lock_guard<std::mutex> wlock(_this_lock);
    filesMap->clear();
    _CLDELETE(lockFactory);
    _CLDELETE(filesMap);
}

void DorisRAMFSDirectory::init(const io::FileSystemSPtr& fs, const char* path,
                               lucene::store::LockFactory* lock_factory) {
    _fs = fs;
    directory = path;

    lucene::store::Directory::setLockFactory(_CLNEW lucene::store::SingleInstanceLockFactory());
}

bool DorisRAMFSDirectory::list(std::vector<std::string>* names) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->begin();
    while (itr != filesMap->end()) {
        names->emplace_back(itr->first);
        ++itr;
    }
    return true;
}

bool DorisRAMFSDirectory::fileExists(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    return filesMap->exists((char*)name);
}

int64_t DorisRAMFSDirectory::fileModified(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::fileModified_file_not_found", { f = nullptr; })
    if (f == nullptr) {
        _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
    }
    return f->getLastModified();
}

void DorisRAMFSDirectory::touchFile(const char* name) {
    lucene::store::RAMFile* file = nullptr;
    {
        std::lock_guard<std::mutex> wlock(_this_lock);
        file = filesMap->get((char*)name);
        DBUG_EXECUTE_IF("DorisRAMFSDirectory::touchFile_file_not_found", { file = nullptr; })
        if (file == nullptr) {
            _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
        }
    }
    const uint64_t ts1 = file->getLastModified();
    uint64_t ts2 = lucene::util::Misc::currentTimeMillis();

    //make sure that the time has actually changed
    while (ts1 == ts2) {
        _LUCENE_SLEEP(1);
        ts2 = lucene::util::Misc::currentTimeMillis();
    };

    file->setLastModified(ts2);
}

int64_t DorisRAMFSDirectory::fileLength(const char* name) const {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* f = filesMap->get((char*)name);
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::fileLength_file_not_found", { f = nullptr; })
    if (f == nullptr) {
        _CLTHROWA(CL_ERR_IO, fmt::format("NOT FOUND File {}.", name).c_str());
    }
    return f->getLength();
}

bool DorisRAMFSDirectory::openInput(const char* name, lucene::store::IndexInput*& ret,
                                    CLuceneError& error, int32_t bufferSize) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto* file = filesMap->get((char*)name);
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::openInput_file_not_found", { file = nullptr; })
    if (file == nullptr) {
        error.set(CL_ERR_IO,
                  "[DorisRAMCompoundDirectory::open] The requested file does not exist.");
        return false;
    }
    ret = _CLNEW lucene::store::RAMInputStream(file);
    return true;
}

void DorisRAMFSDirectory::close() {
    DorisFSDirectory::close();
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::close_close_with_error",
                    { _CLTHROWA(CL_ERR_IO, "debug_point: close DorisRAMFSDirectory error"); })
}

bool DorisRAMFSDirectory::doDeleteFile(const char* name) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->find((char*)name);
    if (itr != filesMap->end()) {
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr->second->sizeInBytes;
        filesMap->removeitr(itr);
    }
    return true;
}

bool DorisRAMFSDirectory::deleteDirectory() {
    // do nothing, RAM dir do not have actual files
    return true;
}

void DorisRAMFSDirectory::renameFile(const char* from, const char* to) {
    std::lock_guard<std::mutex> wlock(_this_lock);
    auto itr = filesMap->find((char*)from);

    /* DSR:CL_BUG_LEAK:
    ** If a file named $to already existed, its old value was leaked.
    ** My inclination would be to prevent this implicit deletion with an
    ** exception, but it happens routinely in CLucene's internals (e.g., during
    ** IndexWriter.addIndexes with the file named 'segments'). */
    if (filesMap->exists((char*)to)) {
        auto itr1 = filesMap->find((char*)to);
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= itr1->second->sizeInBytes;
        filesMap->removeitr(itr1);
    }
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::renameFile_itr_filesMap_end", { itr = filesMap->end(); })
    if (itr == filesMap->end()) {
        char tmp[1024];
        snprintf(tmp, 1024, "cannot rename %s, file does not exist", from);
        _CLTHROWT(CL_ERR_IO, tmp);
    }
    DCHECK(itr != filesMap->end());
    auto* file = itr->second;
    filesMap->removeitr(itr, false, true);
    filesMap->put(strdup(to), file);
}

lucene::store::IndexOutput* DorisRAMFSDirectory::createOutput(const char* name) {
    /* Check the $filesMap VoidMap to see if there was a previous file named
    ** $name.  If so, delete the old RAMFile object, but reuse the existing
    ** char buffer ($n) that holds the filename.  If not, duplicate the
    ** supplied filename buffer ($name) and pass ownership of that memory ($n)
    ** to $files. */
    std::lock_guard<std::mutex> wlock(_this_lock);

    // get the actual pointer to the output name
    char* n = nullptr;
    auto itr = filesMap->find(const_cast<char*>(name));
    DBUG_EXECUTE_IF("DorisRAMFSDirectory::createOutput_itr_filesMap_end",
                    { itr = filesMap->end(); })
    if (itr != filesMap->end()) {
        n = itr->first;
        lucene::store::RAMFile* rf = itr->second;
        SCOPED_LOCK_MUTEX(this->THIS_LOCK);
        sizeInBytes -= rf->sizeInBytes;
        _CLDELETE(rf);
    } else {
        n = STRDUP_AtoA(name);
    }

    auto* file = _CLNEW lucene::store::RAMFile();
    (*filesMap)[n] = file;

    return _CLNEW lucene::store::RAMOutputStream(file);
}

std::string DorisRAMFSDirectory::toString() const {
    return std::string("DorisRAMFSDirectory@") + this->directory;
}

const char* DorisRAMFSDirectory::getClassName() {
    return "DorisRAMFSDirectory";
}

const char* DorisRAMFSDirectory::getObjectName() const {
    return getClassName();
}

DorisFSDirectory* DorisFSDirectoryFactory::getDirectory(const io::FileSystemSPtr& _fs,
                                                        const char* _file, bool can_use_ram_dir,
                                                        lucene::store::LockFactory* lock_factory) {
    DorisFSDirectory* dir = nullptr;
    DBUG_EXECUTE_IF("DorisFSDirectoryFactory::getDirectory_file_is_nullptr", { _file = nullptr; });
    if (!_file || !*_file) {
        _CLTHROWA(CL_ERR_IO, "Invalid directory");
    }

    const char* file = _file;

    // Write by RAM directory
    // 1. only write separated index files, which is can_use_ram_dir = true.
    // 2. config::inverted_index_ram_dir_enable = true
    if (config::inverted_index_ram_dir_enable && can_use_ram_dir) {
        dir = _CLNEW DorisRAMFSDirectory();
    } else {
        bool exists = false;
        auto st = _fs->exists(file, &exists);
        DBUG_EXECUTE_IF("DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok", {
            st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "debug point: DorisFSDirectoryFactory::getDirectory_exists_status_is_not_ok");
        })
        LOG_AND_THROW_IF_ERROR(st, "Get directory exists IO error");
        if (!exists) {
            st = _fs->create_directory(file);
            DBUG_EXECUTE_IF(
                    "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_not_ok", {
                        st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                "debug point: "
                                "DorisFSDirectoryFactory::getDirectory_create_directory_status_is_"
                                "not_ok");
                    })
            LOG_AND_THROW_IF_ERROR(st, "Get directory create directory IO error");
        }
        dir = _CLNEW DorisFSDirectory();
    }
    dir->init(_fs, file, lock_factory);

    return dir;
}

} // namespace doris::segment_v2
