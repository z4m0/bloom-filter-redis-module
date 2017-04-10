#include "redismodule.h"
#include "murmur.h"
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <math.h>

#define DEFAULT_CAPACITY 1000000;
#define DEFAULT_ERROR_RATE 0.01;

uint32_t SEEDS[100] = {
  15766226,74851606,56934393,93602938,65460882,
  74716504,2706207,82022801,73919934,81200056,
  25922260,71173083,35347855,68586037,70974759,
  59750831,28944123,44189824,64847079,40485340,
  94838647,1730654,82480656,42318171,54340928,
  95403952,9457878,11781902,89282531,69986542,
  32796318,9113199,13240675,75855269,15800378,
  48280550,48686482,69253737,44154259,60992987,
  4344081,36440154,87067658,52019066,3731283,
  10121861,84087180,38278455,19489090,28030746,
  59953244,49132922,66411905,39168390,27311396,
  54362764,85005932,44538268,15407555,39732788,
  96661928,41406270,27139095,12609460,98566678,
  22107564,38362487,3239856,56979281,7455893,
  61980987,84039766,27488181,96874559,19896568,
  18803538,89491779,77927728,88620814,41927712,
  53288039,66460458,50700738,57519005,42830844,
  67262694,14268194,38176283,49570511,85979003,
  79702485,80834338,36549018,405149,72562839,
  92596757,10588781,32693308,67664291,57137030
};


typedef struct {
  long long seed;
  float error_rate;
  uint64_t capacity;
} header_t;

/* Return the UNIX time in microseconds */
long long ustime(void) {
  struct timeval tv;
  long long ust;

  gettimeofday(&tv, NULL);
  ust = ((long long)tv.tv_sec) * 1000000;
  ust += tv.tv_usec;
  return ust;
}

uint optimalM(double n, double p){
  return ceil(1.44 * n *  log2(1 / p));
}

uint optimalK(double M, double n){
  return ceil(0.69 * M / n);
}

void addElement(const header_t* header, const char *element, size_t len, char *bfilter, uint64_t bfilter_len, RedisModuleCtx *ctx){
  uint64_t out[2];
  uint k = optimalK(optimalM(header->capacity, header->error_rate), header->capacity);
  for(int i=0; i<k; ++i){
    MurmurHash3_x64_128(element, len, SEEDS[i] + header->seed, &out);
    uint pos =  out[0] % bfilter_len;
    //set the pos in the filter to 1
    bfilter[pos / 8] |= 1 << (pos % 8);
  }
}

int existsElement(const header_t* header, const char *element, size_t len, char *bfilter, uint64_t bfilter_len){
  uint64_t out[2];
  uint k = optimalK(optimalM(header->capacity, header->error_rate), header->capacity);
  for(int i=0; i<k; ++i) {
    MurmurHash3_x64_128(element, len, SEEDS[i] + header->seed, &out);
    uint64_t pos =  out[0] % bfilter_len;
    //get the value of the filter at pos
    if(!(bfilter[pos / 8] >> (pos % 8)) & 1){
      return 0;
    }
  }
  return 1;
}

int merge_bfilter(const header_t* header1, char *bfilter1, uint64_t bfilter_len1, const header_t* header2, char *bfilter2, uint64_t bfilter_len2){
  if(bfilter_len1 != bfilter_len2 || header1->seed != header2->seed || header1->capacity != header2->capacity || header1->error_rate != header2->error_rate){
    return -1;
  }
  for(int i=0; i<bfilter_len1; ++i){
    bfilter1[i] |= bfilter2[i];
  }
  return 0;
}


int CreateBloomFilterCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RedisModule_AutoMemory(ctx);
  RedisModuleKey *key;
  key = RedisModule_OpenKey(ctx,argv[1],REDISMODULE_READ | REDISMODULE_WRITE);
  if (argc < 2) return RedisModule_WrongArity(ctx);

  long long capacity = DEFAULT_CAPACITY;
  if(argc >= 3){
    if(RedisModule_StringToLongLong(argv[2], &capacity) != REDISMODULE_OK){
      return RedisModule_ReplyWithError(ctx,"Second parameter must be int");
    }
  }

  double error_rate = DEFAULT_ERROR_RATE;
  if(argc >= 4){
    if(RedisModule_StringToDouble(argv[3], &error_rate) != REDISMODULE_OK){
      return RedisModule_ReplyWithError(ctx,"Third parameter must be float");
    }
  }

  long long seed = ustime();
  if(argc >= 5){
    if(RedisModule_StringToLongLong(argv[4], &seed) != REDISMODULE_OK){
      return RedisModule_ReplyWithError(ctx,"Fourth parameter must be int");
    }
  }

  size_t size = optimalM(capacity, error_rate);

  size += sizeof(header_t);
  if(RedisModule_StringTruncate(key, size) == REDISMODULE_ERR){
    return REDISMODULE_ERR;
  }

  char *str = RedisModule_StringDMA(key, &size, REDISMODULE_WRITE);
  memset(str, 0, size);

  header_t* bhead = (header_t*)str;
  bhead->seed = seed;
  bhead->capacity = capacity;
  bhead->error_rate = error_rate;

  RedisModule_CloseKey(key);
  return RedisModule_ReplyWithSimpleString(ctx, "OK");;
}

int AddElementCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  RedisModule_AutoMemory(ctx);
  RedisModuleKey *key;
  key = RedisModule_OpenKey(ctx,argv[1],REDISMODULE_READ | REDISMODULE_WRITE);
  if (argc != 3) return RedisModule_WrongArity(ctx);
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "key is not the correct type");
  }
  size_t element_len;
  const char *element = RedisModule_StringPtrLen(argv[2], &element_len);

  size_t bfsize = RedisModule_ValueLength(key);
  char *ptr = RedisModule_StringDMA(key, &bfsize, REDISMODULE_WRITE);
  addElement((header_t*) ptr, element, element_len, ptr + sizeof(header_t), (bfsize - sizeof(header_t)) * 8, ctx);

  RedisModule_CloseKey(key);
  return RedisModule_ReplyWithSimpleString(ctx,"OK");
}

int ExistsElementCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  RedisModule_AutoMemory(ctx);
  RedisModuleKey *key;
  key = RedisModule_OpenKey(ctx,argv[1],REDISMODULE_READ);
  if (argc != 3) return RedisModule_WrongArity(ctx);
  if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_STRING) {
    RedisModule_CloseKey(key);
    return RedisModule_ReplyWithError(ctx, "key is not the correct type");
  }

  size_t element_len;
  const char *element = RedisModule_StringPtrLen(argv[2], &element_len);

  size_t bfsize = RedisModule_ValueLength(key);
  char *ptr = RedisModule_StringDMA(key, &bfsize, REDISMODULE_READ);
  int exists = existsElement((header_t*) ptr, element, element_len, ptr + sizeof(header_t), (bfsize - sizeof(header_t)) * 8);
  RedisModule_CloseKey(key);
  return RedisModule_ReplyWithLongLong(ctx, exists);
}

int MergeCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc){
  RedisModule_AutoMemory(ctx);
  if (argc != 3) return RedisModule_WrongArity(ctx);
  RedisModuleKey *key1, *key2;
  key1 = RedisModule_OpenKey(ctx,argv[1],REDISMODULE_READ | REDISMODULE_WRITE);
  key2 = RedisModule_OpenKey(ctx,argv[2],REDISMODULE_READ);
  if (RedisModule_KeyType(key1) != REDISMODULE_KEYTYPE_STRING) {
    RedisModule_CloseKey(key1);
    return RedisModule_ReplyWithError(ctx, "key 1 is not the correct type");
  }
  if (RedisModule_KeyType(key2) != REDISMODULE_KEYTYPE_STRING) {
    RedisModule_CloseKey(key2);
    return RedisModule_ReplyWithError(ctx, "key 2 is not the correct type");
  }
  size_t bfsize1 = RedisModule_ValueLength(key1);
  char *ptr1 = RedisModule_StringDMA(key1, &bfsize1, REDISMODULE_READ | REDISMODULE_WRITE);

  size_t bfsize2 = RedisModule_ValueLength(key2);
  char *ptr2 = RedisModule_StringDMA(key2, &bfsize2, REDISMODULE_READ);

  int res = merge_bfilter((header_t*) ptr1, ptr1 + sizeof(header_t), (bfsize1 - sizeof(header_t)) * 8, (header_t*) ptr2, ptr2 + sizeof(header_t), (bfsize2 - sizeof(header_t)) * 8);

  RedisModule_CloseKey(key1);
  RedisModule_CloseKey(key2);
  if(res < 0){
    return RedisModule_ReplyWithError(ctx, "bfilters don't have the same parameters");
  }

  return RedisModule_ReplyWithSimpleString(ctx,"OK");
}


int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"bloomfilter",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bf.init",
        CreateBloomFilterCommand,"write fast deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bf.add",
        AddElementCommand,"write fast deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bf.exists",
        ExistsElementCommand,"write fast deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bf.merge",
        MergeCommand,"write fast deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    return REDISMODULE_OK;
}
