/*
 * Copyright 2011 Canonical Ltd.
 *
 * This file is part of u1db.
 *
 * u1db is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Lesser General Public License version 3
 * as published by the Free Software Foundation.
 *
 * u1db is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License
 * along with u1db.  If not, see <http://www.gnu.org/licenses/>.
 */

#include <string.h>
#include "u1db/u1db_internal.h"


#if defined(_WIN32) || defined(WIN32)
#include "Wincrypt.h"

static HCRYPTPROV crypt_provider = 0;

static HCRYPTPROV get_provider()
{
    // Note: There is a small potential thread-race condition as crypt_provider
    //       is a global, if we adopt a threading lib, consider adding a lock.
    if (crypt_provider == 0) {
        if (!CryptAcquireContext(&crypt_provider, NULL, NULL, PROV_RSA_AES,
                                 CRYPT_VERIFYCONTEXT))
        {
            return 0;
        }
    }
    return crypt_provider;
}

int
u1db__random_bytes(void *buf, size_t count)
{
    HCRYPTPROV provider;

    provider = get_provider();
    if (provider == 0) {
        // TODO: This is really system failure, but we'll go with Invalid
        //       Parameter for now.
        return U1DB_INVALID_PARAMETER;
    }
    if (!CryptGenRandom(provider, count, (BYTE*)buf)) {
        // TODO: Probably want a better error here.
        return U1DB_NOMEM;
    }
    return U1DB_OK;
}

#else

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
// We leave the file handle open, and let the process closing close it.
static int urandom_fd = -1;

static int
get_urandom_fd(void)
{
    // Note: There is a small potential thread-race condition as urandom_fd
    //       is a global, if we adopt a threading lib, consider adding a lock.
    if (urandom_fd < 0) {
        urandom_fd = open("/dev/urandom", O_RDONLY);
    }
    return urandom_fd;
}

int
u1db__random_bytes(void *buf, size_t count)
{
    int fd, n;
    fd = get_urandom_fd();
    if (fd == -1) {
        return errno;
    }
    n = read(fd, buf, count);
    if (n < count) {
        return errno;
    }
    return U1DB_OK;
}

#endif // defined(_WIN32) || defined(WIN32)


int
u1db__generate_hex_uuid(char *uuid)
{
    unsigned char buf[16] = {0};
    u1db__random_bytes(buf, 16);
    // We set the version number to 4
    buf[6] = (buf[6] & 0x0F) | 0x40;
    // And for the clock bits, bit 6 is 0, bit 7 is 1
    buf[8] = (buf[8] & 0x3F) | 0x80;
    u1db__bin_to_hex(buf, 16, uuid);
    return U1DB_OK;
}

void
u1db__bin_to_hex(unsigned char *bin_in, int bin_count, char *hex_out)
{
    int i;
    for (i = 0; i < bin_count; ++i) {
        hex_out[i*2] = (bin_in[i] >> 4);
        hex_out[i*2+1] = (bin_in[i] & 0x0F);
    }
    for (i = 0; i < bin_count*2; ++i) {
        if (hex_out[i] < 10) {
            hex_out[i] += '0';
        } else {
            hex_out[i] += 'a' - 10;
        }
    }
}
