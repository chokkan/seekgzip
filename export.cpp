#include <string>
#include <stdexcept>
#include "seekgzip.h"
#include "export.h"

static std::string error_string(int errorcode)
{
    switch (errorcode) {
    case SEEKGZIP_SUCCESS:
        return "";
    case SEEKGZIP_OPENERROR:
        return "Failed to open a file";
    case SEEKGZIP_READERROR:
        return "Failed to read from a file";
    case SEEKGZIP_WRITEERROR:
        return "Failed to write to a file";
    case SEEKGZIP_DATAERROR:
        return "Data error";
    case SEEKGZIP_OUTOFMEMORY:
        return "Out of memory";
    case SEEKGZIP_IMCOMPATIBLE:
        return "Imcompatible data format";
    case SEEKGZIP_ZLIBERROR:
        return "ZLIB error";
    default:
    case SEEKGZIP_ERROR:
        return "Unknown error";
    }
}

reader::reader(const char *filename)
{
    int err = 0;
    seekgzip_t* sgz = seekgzip_open(filename, &err);
    m_obj = sgz;
    if (sgz == NULL) {
        throw std::invalid_argument(error_string(err));
    }
}

reader::~reader()
{
    this->close();
}

void reader::close()
{
    if (m_obj != NULL) {
        seekgzip_close(reinterpret_cast<seekgzip_t*>(m_obj));
        m_obj = NULL;
    }
}

void reader::seek(long long offset)
{
    if (m_obj != NULL) {
        seekgzip_seek(
            reinterpret_cast<seekgzip_t*>(m_obj),
            offset
            );
    }
}

long long reader::tell()
{
    if (m_obj != NULL) {
        return seekgzip_tell(
            reinterpret_cast<seekgzip_t*>(m_obj)
            );
    } else {
        return -1;    
    }
}

std::string reader::read(int size)
{
    std::string ret;
    if (m_obj != NULL) {
        char *buffer = new char[size+1];
        int n = seekgzip_read(
            reinterpret_cast<seekgzip_t*>(m_obj),
            buffer,
            size
            );
        buffer[n] = 0;
        ret = buffer;
        delete[] buffer;
    }
    return ret;
}

