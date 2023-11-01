#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
// #include "erasure_code.h"	// use <isa-l.h> instead when linking against installed
#include <isa-l.h>
#include <jni.h>
#include <string.h>
#include <unistd.h>

#include <time.h>
#include <sched.h>
#include <pthread.h>
#include "org_apache_crail_tools_Crailcoding.h"

// Some auxiliary variates for async coding
typedef unsigned char u8;
#define KMAX 255
#define MMAX 10000

static int gf_gen_decode_matrix_microec(u8 *encode_matrix,
										u8 *decode_matrix,
										u8 *invert_matrix,
										u8 *temp_matrix,
										int *decode_index, u8 *frag_err_list, int nerrs, int k,
										int m);

int verifyMatrix(u8 *arr1, u8 *arr2, int len)
{
    int i;
    for (i = 0; i < len; i++)
    {
        if (arr1[i] != arr2[i])
        {
            return 0;
        }
    }
    return 1;
}

JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_WarmCall(JNIEnv *env, jobject obj)
{

}

// encode_matrix len: m*k; g_tbls len: k*p*32
JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_InitAuxiliaries(JNIEnv *env, jobject obj, jint NumDataBlock, jint NumParityBlock, jobject buffer_encode_matrix, jobject buffer_g_tbls)
{
    int k = NumDataBlock, p = NumParityBlock, m = k + p;
    u8 *encode_matrix = (u8 *)env->GetDirectBufferAddress(buffer_encode_matrix);
    u8 *g_tbls = (u8 *)env->GetDirectBufferAddress(buffer_g_tbls);

    gf_gen_cauchy1_matrix(encode_matrix, m, k);
    ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);
}

// test for InitAuxiliaries
JNIEXPORT int JNICALL Java_org_apache_crail_tools_Crailcoding_TestInitAuxiliaries(JNIEnv *env, jobject obj, jint NumDataBlock, jint NumParityBlock, jobject buffer_encode_matrix, jobject buffer_g_tbls)
{
    int k = NumDataBlock, p = NumParityBlock, m = k + p;
    u8 *test_encode_matrix = (u8 *)env->GetDirectBufferAddress(buffer_encode_matrix);
    u8 *test_g_tbls = (u8 *)env->GetDirectBufferAddress(buffer_g_tbls);

    u8 *encode_matrix;
    u8 *g_tbls;
    encode_matrix = (u8 *)malloc(m * k);
    g_tbls = (u8 *)malloc(k * p * 32);
    gf_gen_cauchy1_matrix(encode_matrix, m, k);
    ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

    int ret = memcmp(encode_matrix, test_encode_matrix, m * k);
    ret == 0 ? printf("Right encode_matrix!\n") : printf("Error encode_matrix!\n");
    ret = memcmp(g_tbls, test_g_tbls, k * p * 32);
    ret == 0 ? printf("Right g_tbls!\n") : printf("Error g_tbls!\n");

    return ret;
}


JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_MicroecEncoding(JNIEnv *env, jobject obj, jobjectArray data, jobjectArray parity, jobject cursor, jint NumSubStripe, jint NumDataBlock, jint NumParityBlock, jint Microec_buffer_size, jint NumCore)
{
	// set affinity
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(NumCore, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
	{
		fprintf(stderr, "warning: could not set CPU affinity\n");
	}

	/* Initialize some paras*/
	// k: # data blocks, p: # parity blocks, len: size of block
	int k = NumDataBlock, p = NumParityBlock, split = NumSubStripe, len = Microec_buffer_size / split;
	int m = k + p;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];
	int i;

	// data buffer
	for (i = 0; i < NumDataBlock; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(data, i));
	}

	// parity buffer
	for (i = NumDataBlock; i < NumDataBlock + NumParityBlock; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(parity, i - NumDataBlock));
	}

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1)
	{
		printf(" Input test parameter error m=%d, k=%d, p=%d\n", m, k, p);
	}

	// Allocate coding matrices
	encode_matrix = (u8 *)malloc(m * k);
	g_tbls = (u8 *)malloc(k * p * 32);

	if (encode_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return;
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// Initialize g_tbls from encode matrix
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	int j;
    u8 *cursor_used = (u8 *)env->GetDirectBufferAddress(cursor);

    for (j = 0; j < split; j++)
    {
        ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
        cursor_used[j] = '1';

        // Update parity & src buffers
        if (j < split - 1)
        {
            for (i = 0; i < m; i++)
            {
                frag_ptrs[i] = frag_ptrs[i] + len;
            }
        }
    }

	free(encode_matrix);
	free(g_tbls);
	return;
}

JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_PureMicroecEncoding(JNIEnv *env, jobject obj, jobjectArray data, jobjectArray parity, jint NumSubStripe, jint NumDataBlock, jint NumParityBlock, jint Microec_buffer_size, jint NumCore)
{
	// set affinity
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(NumCore, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
	{
		fprintf(stderr, "warning: could not set CPU affinity\n");
	}

	/* Initialize some paras*/
	// k: # data blocks, p: # parity blocks, len: size of block
	int k = NumDataBlock, p = NumParityBlock, split = NumSubStripe, len = Microec_buffer_size / split;
	int m = k + p;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];
	int i;

	// data buffer
	for (i = 0; i < NumDataBlock; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(data, i));
	}

	// parity buffer
	for (i = NumDataBlock; i < NumDataBlock + NumParityBlock; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(parity, i - NumDataBlock));
	}

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1)
	{
		printf(" Input test parameter error m=%d, k=%d, p=%d\n", m, k, p);
	}

	// Allocate coding matrices
	encode_matrix = (u8 *)malloc(m * k);
	g_tbls = (u8 *)malloc(k * p * 32);

	if (encode_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return;
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// Initialize g_tbls from encode matrix
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	int j;
	for (j = 0; j < split; j++)
	{
		ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);

		// Update parity & src buffers
		if (j < split - 1)
		{
			for (i = 0; i < m; i++)
			{
				frag_ptrs[i] = frag_ptrs[i] + len;
			}
		}
	}
	free(encode_matrix);
	free(g_tbls);
	return;
}

JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_NativeEncoding(JNIEnv *env, jobject obj, jobjectArray data, jobjectArray parity, jint NumDataBlock, jint NumParityBlock, jint Naive_buffer_size, jint NumCore)
{
    // set affinity
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(NumCore, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        fprintf(stderr, "warning: could not set CPU affinity\n");
    }

	// k: # data blocks, p: # parity blocks, len: size of block
	int k = NumDataBlock, p = NumParityBlock, len = Naive_buffer_size;
	int i, j, m;
	m = k + p;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1)
	{
		printf(" Input test parameter error m=%d, k=%d, p=%d \n", m, k, p);
	}

	// Allocate coding matrices
	encode_matrix = (u8 *)malloc(m * k);
	g_tbls = (u8 *)malloc(k * p * 32);

	// Check for valid parameters
	if (encode_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return;
	}

	// data buffer
	for (i = 0; i < k; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(data, i));
	}

	// parity buffer
	for (i = k; i < m; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(parity, i - k));
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// Initialize g_tbls from encode matrix
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	// Generate EC parity blocks from sources
	ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);

	free(encode_matrix);
	free(g_tbls);

	return;
}

void setHasCodedNum(u8* hasCoded, int computed)
{
    hasCoded[0] = computed / 127;
    hasCoded[1] = computed % 127;
//    printf("setHasCodedNum final %d,%d\n", hasCoded[0], hasCoded[1]);
}

JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_MicroecDecoding(JNIEnv *env, jobject obj, jobjectArray Src, jobjectArray EraseData, jintArray SrcIndex, jintArray EraseIndex, jobject cursor, jint NumSubStripe, jint NumDataBlock, jint NumParityBlock, jint Microec_buffer_size, jint NumEraseData, jint NumCore, jobject hasTransfered_buf, jobject hasCoded_buf)
{
	// set affinity
	cpu_set_t mask;
	CPU_ZERO(&mask);
	CPU_SET(NumCore, &mask);
	if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
	{
		fprintf(stderr, "warning: could not set CPU affinity\n");
	}

	// k: # data blocks, p: # parity blocks, len: size of block
	int k = NumDataBlock, p = NumParityBlock, len = Microec_buffer_size / NumSubStripe, split = NumSubStripe;
	int i, j, m, e, ret, c;
	m = k + p;
	int nerrs = NumEraseData;
	int transfered = 0;
	int computed = 0;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];
	u8 *recover_outp[KMAX];
	u8 frag_err_list[KMAX];
	u8 *tmp[KMAX];

	// Coefficient matrices
	u8 *encode_matrix, *decode_matrix;
	u8 *invert_matrix;
	u8 *temp_matrix;
	u8 *g_tbls;
	int decode_index[KMAX];

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1)
	{
		printf(" Input test parameter error m=%d, k=%d, p=%d, erasures=%d\n", m, k, p, nerrs);
	}

	// Allocate coding matrices
	encode_matrix = (u8 *)malloc(m * k);
	decode_matrix = (u8 *)malloc(m * k);
	invert_matrix = (u8 *)malloc(m * k);
	temp_matrix = (u8 *)malloc(m * k);
	g_tbls = (u8 *)malloc(k * p * 32);

	// data buffers
	for (i = 0; i < k; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(Src, i));
	}

	// Erase buffers
	for (i = 0; i < NumEraseData; i++)
	{
		recover_outp[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(EraseData, i));
	}

	// Cursor buffer
	u8 *cursor_used = (u8 *)env->GetDirectBufferAddress(cursor);
	u8 *hasTransfered = (u8 *)env->GetDirectBufferAddress(hasTransfered_buf);
	u8 *hasCoded = (u8 *)env->GetDirectBufferAddress(hasCoded_buf);

	if (encode_matrix == NULL || decode_matrix == NULL || invert_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return;
	}
	if (nerrs <= 0)
		return;

	// Get failed ids
	jint *tmp_EraseIndex, *tmp_SrcIndex;
	tmp_EraseIndex = env->GetIntArrayElements(EraseIndex, NULL);
	for (i = 0; i < NumEraseData; i++)
	{
		frag_err_list[i] = tmp_EraseIndex[i];
	}
	tmp_SrcIndex = env->GetIntArrayElements(SrcIndex, NULL);
	for (i = 0; i < k; i++)
	{
		decode_index[i] = tmp_SrcIndex[i];
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// Find a decode matrix to regenerate all erasures from remaining frags
	ret = gf_gen_decode_matrix_microec(encode_matrix, decode_matrix,
									   invert_matrix, temp_matrix, decode_index,
									   frag_err_list, nerrs, k, m);
	if (ret != 0)
	{
		printf("Fail on generate decode matrix\n");
		return;
	}

	// Recover data
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);


	j = 0;
	// Existing bug: while (j < split || j < cursor_network_used)
	// Decoding will ignore the cursor_network_used and keep decoding until all sub-stripe finishing
	// Fix: keep decoding when j < cursor_network_used, and finish decoding until j==split

	while (j < split)
	{
	    transfered = hasTransfered[0] * 127 + hasTransfered[1];
//	    printf("microec.c transfered %d\n", transfered);

	    if (j < transfered)
	    {
            ec_encode_data(len, k, nerrs, g_tbls, frag_ptrs, recover_outp);
            cursor_used[j] = '1';
            computed++;
//            printf("microec.c computed %d\n", computed);
            setHasCodedNum(hasCoded, computed);

            if (j < split - 1)
            {
                // Update the src buffers
                for (i = 0; i < k; i++)
                {
                    frag_ptrs[i] = frag_ptrs[i] + len;
                }
                // Update the erase buffers
                for (i = 0; i < NumEraseData; i++)
                {
                    recover_outp[i] = recover_outp[i] + len;
                }
            }
            j++;
	    }
	}

	free(encode_matrix);
	free(decode_matrix);
	free(invert_matrix);
	free(temp_matrix);
	free(g_tbls);
	return;
}

JNIEXPORT void JNICALL Java_org_apache_crail_tools_Crailcoding_NativeDecoding(JNIEnv *env, jobject obj, jobjectArray Src, jobjectArray EraseData, jintArray SrcIndex, jintArray EraseIndex, jint NumDataBlock, jint NumParityBlock, jint Naive_buffer_size, jint NumEraseData, jint NumCore)
{
    // set affinity
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(NumCore, &mask);
    if (sched_setaffinity(0, sizeof(mask), &mask) == -1)
    {
        fprintf(stderr, "warning: could not set CPU affinity\n");
    }

	// k: # data blocks, p: # parity blocks, len: size of block
	int k = NumDataBlock, p = NumParityBlock, len = Naive_buffer_size;
	int i, j, m, e, ret, c;
	m = k + p;
	int nerrs = NumEraseData;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];
	u8 *recover_outp[KMAX];
	u8 frag_err_list[KMAX];
	u8 *tmp[KMAX];

	// Coefficient matrices
	u8 *encode_matrix, *decode_matrix;
	u8 *invert_matrix;
	u8 *temp_matrix;
	u8 *g_tbls;
	int decode_index[KMAX];

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1)
	{
		printf(" Input test parameter error m=%d, k=%d, p=%d, erasures=%d\n", m, k, p, nerrs);
	}

	// Allocate coding matrices
	encode_matrix = (u8 *)malloc(m * k);
	decode_matrix = (u8 *)malloc(m * k);
	invert_matrix = (u8 *)malloc(m * k);
	temp_matrix = (u8 *)malloc(m * k);
	g_tbls = (u8 *)malloc(k * p * 32);

	// data buffer
	for (i = 0; i < k; i++)
	{
		frag_ptrs[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(Src, i));
	}
	// Erase buffer
	for (i = 0; i < NumEraseData; i++)
	{
		recover_outp[i] = (u8 *)env->GetDirectBufferAddress(env->GetObjectArrayElement(EraseData, i));
	}

	if (encode_matrix == NULL || decode_matrix == NULL || invert_matrix == NULL || g_tbls == NULL)
	{
		printf("Test failure! Error with malloc\n");
		return;
	}

	// Get failed ids
	jint *tmp_EraseIndex, *tmp_SrcIndex;
	tmp_EraseIndex = env->GetIntArrayElements(EraseIndex, NULL);
	for (i = 0; i < NumEraseData; i++)
	{
		frag_err_list[i] = tmp_EraseIndex[i];
	}
	tmp_SrcIndex = env->GetIntArrayElements(SrcIndex, NULL);
	for (i = 0; i < k; i++)
	{
		decode_index[i] = tmp_SrcIndex[i];
	}

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	// recovery from here
	if (nerrs <= 0)
		return;

	// Find a decode matrix to regenerate all erasures from remaining frags
	ret = gf_gen_decode_matrix_microec(encode_matrix, decode_matrix,
									   invert_matrix, temp_matrix, decode_index,
									   frag_err_list, nerrs, k, m);
	if (ret != 0)
	{
		printf("Fail on generate decode matrix\n");
		return;
	}

	ec_init_tables(k, nerrs, decode_matrix, g_tbls);

	// Recover data
	ec_encode_data(len, k, nerrs, g_tbls, frag_ptrs, recover_outp);

	free(encode_matrix);
	free(decode_matrix);
	free(invert_matrix);
	free(temp_matrix);
	free(g_tbls);
	return;
}

// Generate decode matrix from encode matrix and erasure list
static int gf_gen_decode_matrix_microec(u8 *encode_matrix,
										u8 *decode_matrix,
										u8 *invert_matrix,
										u8 *temp_matrix,
										int *decode_index, u8 *frag_err_list, int nerrs, int k,
										int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[KMAX];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++)
	{
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0; i < k; i++)
	{
		r = decode_index[i];
		for (j = 0; j < k; j++)
		{
			b[k * i + j] = encode_matrix[k * r + j];
		}
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++)
	{
		if (frag_err_list[i] < k) // A src err
			for (j = 0; j < k; j++)
			{
				decode_matrix[k * i + j] = invert_matrix[k * frag_err_list[i] + j];
			}
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++)
	{
		if (frag_err_list[p] >= k)
		{
			for (i = 0; i < k; i++)
			{
				s = 0;
				for (j = 0; j < k; j++)
					s ^= gf_mul(invert_matrix[j * k + i],
								encode_matrix[k * frag_err_list[p] + j]);
				decode_matrix[k * p + i] = s;
			}
		}
	}
	return 0;
}