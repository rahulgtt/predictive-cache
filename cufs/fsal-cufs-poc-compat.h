#pragma once

#include_next <cufs/fsal_cufs/fsal_cufs.h>

typedef struct stat cufs_stat_t;

#ifndef st_btim
#define st_btim st_ctim
#endif
