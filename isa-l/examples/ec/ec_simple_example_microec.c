/**********************************************************************
  Copyright(c) 2011-2018 Intel Corporation All rights reserved.

  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions
  are met:
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
    * Redistributions in binary form must reproduce the above copyright
      notice, this list of conditions and the following disclaimer in
      the documentation and/or other materials provided with the
      distribution.
    * Neither the name of Intel Corporation nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.

  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************************/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <getopt.h>
// #include "erasure_code.h"	// use <isa-l.h> instead when linking against installed
#include <isa-l.h>

#include <time.h>
#include <sched.h>
#include <pthread.h>
#include <unistd.h>

#define MMAX 255
#define KMAX 255
#define THREAD_MAX_NUM 1

typedef unsigned char u8;

int usage(void)
{
	fprintf(stderr,
		"Usage: ec_simple_example [options]\n"
		"  -h        Help\n"
		"  -k <val>  Number of source fragments\n"
		"  -p <val>  Number of parity fragments\n"
		"  -l <val>  Length of fragments\n"
		"  -e <val>  Simulate erasure on frag index val. Zero based. Can be repeated.\n"
		"  -r <seed> Pick random (k, p) with seed\n");
	exit(0);
}

static int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index,
				       u8 * frag_err_list, int nerrs, int k, int m);


long MicroEC_encode_test(int k, int p, int len){

	// prepare
	int packet = 64;
	len=len/packet;
	u8 *test =  malloc(1000);
	memset(test,1,1000);
	long sum = 0;
	struct timespec time1 = {0, 0};
    struct timespec time2 = {0, 0};
	struct timespec time3 = {0, 0};
    struct timespec time4 = {0, 0};	
	struct timespec time5 = {0, 0};	
	u8 *data_frag_ptrs[MMAX];
	for (int i = 0; i < (k+p); i++){
		data_frag_ptrs[i]=malloc(len);
		memset(data_frag_ptrs[i],1,len);		
	}


	// start test
	clock_gettime(CLOCK_REALTIME, &time1);
	int i, j, m, c, e, ret;

	long allocate_buffer = 0, cauchy_matrix = 0, en_init = 0, en_code = 0;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;

	m = k + p;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1) {
		printf(" Input test parameter error m=%d, k=%d, p=%d\n",
		       m, k, p);
		usage();
	}

	// Allocate coding matrices
	encode_matrix = malloc(m * k);
	g_tbls = malloc(k * p * 32);

	for (i = 0; i < m; i++) {
		if (NULL == (frag_ptrs[i] = malloc(len))) {
			printf("alloc error: Fail\n");
			return -1;
		}
	}
		
	// Allocate the src & parity buffers
	// clock_gettime(CLOCK_REALTIME, &time2);
	for (i = 0; i < k; i++)
		frag_ptrs[i]=data_frag_ptrs[i];

	// clock_gettime(CLOCK_REALTIME, &time2);
	// printf("allocate buffer time is %ld\n",time2.tv_nsec-time1.tv_nsec);	


	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
	for (i = k; i < m; i++) { 
		memcpy(data_frag_ptrs[i],frag_ptrs[i],len); 
	}
	
	clock_gettime(CLOCK_REALTIME, &time3);
	sum = sum + time3.tv_nsec-time1.tv_nsec;
	// printf("first coding is %ld\n",sum);

	for(int test=0; test<packet-1;test++){
		clock_gettime(CLOCK_REALTIME, &time1);
		for (i = 0; i < k; i++) {
			// printf("frag_ptrs[i] is %d\n", frag_ptrs[i]);
			// free(frag_ptrs[i]);
			if (NULL == (frag_ptrs[i] = data_frag_ptrs[i])) {
				printf("alloc error: Fail\n");
				return -1;
			}
			// memset(frag_ptrs[i],1,len);
		}
		// clock_gettime(CLOCK_REALTIME, &time1);		
		ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
		for (i = k; i < m; i++) { 
			memcpy(data_frag_ptrs[i],frag_ptrs[i],len); 
		}
		clock_gettime(CLOCK_REALTIME, &time2);

		en_code = time2.tv_nsec-time1.tv_nsec;
		sum = sum + en_code;
		// printf("latter coding is %ld\n", en_code);
	 }	 
	//  printf("sum time is %ld\n", sum);

	return sum;
}

long NaiveEC_encode_test(int k, int p, int len){
	u8 *test =  malloc(1000);
	memset(test,1,1000);
	long sum = 0;
	struct timespec time1 = {0, 0};
    struct timespec time2 = {0, 0};
	struct timespec time3 = {0, 0};
    struct timespec time4 = {0, 0};	
	struct timespec time5 = {0, 0};	
	u8 *data_frag_ptrs[MMAX];

	for (int i = 0; i < (k+p); i++){
		data_frag_ptrs[i]=malloc(len);
		memset(data_frag_ptrs[i],1,len);		
	}

	// test start
	clock_gettime(CLOCK_REALTIME, &time1);
	int i, j, m, c, e, ret;

	long allocate_buffer = 0, cauchy_matrix = 0, en_init = 0, en_code = 0;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];

	// Coefficient matrices
	u8 *encode_matrix;
	u8 *g_tbls;

	m = k + p;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1) {
		printf(" Input test parameter error m=%d, k=%d, p=%d\n",
		       m, k, p);
		usage();
	}

	// Allocate coding matrices
	encode_matrix = malloc(m * k);
	g_tbls = malloc(k * p * 32);

	clock_gettime(CLOCK_REALTIME, &time1);
	for (i = 0; i < m; i++) {
		if (NULL == (frag_ptrs[i] = malloc(len))) {
			printf("alloc error: Fail\n");
			return -1;
		}
	}
	clock_gettime(CLOCK_REALTIME, &time2);
	printf("allocate buffer time is %ld\n",time2.tv_nsec-time1.tv_nsec);	
		
	// Allocate the src & parity buffers
	// clock_gettime(CLOCK_REALTIME, &time3);
	for (i = 0; i < k; i++)
		frag_ptrs[i]=data_frag_ptrs[i];

	// clock_gettime(CLOCK_REALTIME, &time2);
	// printf("allocate buffer time is %ld\n",time2.tv_nsec-time1.tv_nsec);	

	gf_gen_cauchy1_matrix(encode_matrix, m, k);

	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);

	ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
	for (i = k; i < m; i++) { 
		memcpy(data_frag_ptrs[i],frag_ptrs[i],len); 
	}
	
	// clock_gettime(CLOCK_REALTIME, &time2);
	// en_code = time2.tv_nsec-time1.tv_nsec;
	// long sum = 0;
	clock_gettime(CLOCK_REALTIME, &time3);
	sum = sum + time3.tv_nsec-time1.tv_nsec;
	// sum = sum + time2.tv_nsec-time1.tv_nsec;
	// printf("first coding is %ld\n",sum);
	
	return sum;
}

void encode_test_legacy(int k, int p, int len){
	int i, j, m, c, e, ret;
	// int k = 4, p = 2;
	//int len = all_len[10] * 1024/k ;	// Default params
	int nerrs = 1;
	
	struct timespec time1 = {0, 0};
    struct timespec time2 = {0, 0};

	long allocate_buffer = 0, cauchy_matrix = 0, en_init = 0, en_code = 0, de_code = 0;

	// Fragment buffer pointers
	u8 *frag_ptrs[MMAX];
	u8 *recover_srcs[KMAX];
	u8 *recover_outp[KMAX];
	u8 frag_err_list[MMAX];
	frag_err_list[0]=0;

	// Coefficient matrices
	u8 *encode_matrix, *decode_matrix;
	u8 *invert_matrix, *temp_matrix;
	u8 *g_tbls;
	u8 decode_index[MMAX];

	m = k + p;

	// Check for valid parameters
	if (m > MMAX || k > KMAX || m < 0 || p < 1 || k < 1) {
		printf(" Input test parameter error m=%d, k=%d, p=%d, erasures=%d\n",
		       m, k, p, nerrs);
		usage();
	}
	if (nerrs > p) {
		printf(" Number of erasures chosen exceeds power of code erasures=%d p=%d\n",
		       nerrs, p);
		usage();
	}
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] >= m) {
			printf(" fragment %d not in range\n", frag_err_list[i]);
			usage();
		}
	}

	printf("ec_simple_example:\n");

	clock_gettime(CLOCK_REALTIME, &time1);
	// Allocate coding matrices
	encode_matrix = malloc(m * k);
	decode_matrix = malloc(m * k);
	invert_matrix = malloc(m * k);
	temp_matrix = malloc(m * k);
	g_tbls = malloc(k * p * 32);
	

	if (encode_matrix == NULL || decode_matrix == NULL
	    || invert_matrix == NULL || temp_matrix == NULL || g_tbls == NULL) {
		printf("Test failure! Error with malloc\n");
		return -1;
	}
	// Allocate the src & parity buffers
	for (i = 0; i < m; i++) {
		if (NULL == (frag_ptrs[i] = malloc(len))) {
			printf("alloc error: Fail\n");
			return -1;
		}
	}
	clock_gettime(CLOCK_REALTIME, &time2);
	
	allocate_buffer = time2.tv_nsec-time1.tv_nsec;

	// clock_gettime(CLOCK_REALTIME, &time1);
	// // Fill sources with random data
	// for (i = 0; i < k; i++)
	// 	for (j = 0; j < len; j++)
	// 		frag_ptrs[i][j] = rand();
	// clock_gettime(CLOCK_REALTIME, &time2);
	// printf("rand() time is %ld\n", time2.tv_nsec-time1.tv_nsec);

	printf(" encode (m,k,p)=(%d,%d,%d) len=%d\n", m, k, p, len);

	// Pick an encode matrix. A Cauchy matrix is a good choice as even
	// large k are always invertable keeping the recovery rule simple.
	clock_gettime(CLOCK_REALTIME, &time1);
	gf_gen_cauchy1_matrix(encode_matrix, m, k);
	clock_gettime(CLOCK_REALTIME, &time2);
	cauchy_matrix = time2.tv_nsec-time1.tv_nsec;

	for(i = 0; i<m; i++){
		for(j=0; j <k; j++){
			printf("%d ", encode_matrix[i*k+j]);
		}
		printf("\n");
	}

	// Initialize g_tbls from encode matrix
	clock_gettime(CLOCK_REALTIME, &time1);
	ec_init_tables(k, p, &encode_matrix[k * k], g_tbls);
	clock_gettime(CLOCK_REALTIME, &time2);
	en_init = time2.tv_nsec-time1.tv_nsec;

	// Generate EC parity blocks from sources
	clock_gettime(CLOCK_REALTIME, &time1);
	ec_encode_data(len, k, p, g_tbls, frag_ptrs, &frag_ptrs[k]);
	clock_gettime(CLOCK_REALTIME, &time2);
	en_code = time2.tv_nsec-time1.tv_nsec;


	// sleep(5);

	printf(" recover %d fragments\n", nerrs);
	clock_gettime(CLOCK_REALTIME, &time1);
	// Find a decode matrix to regenerate all erasures from remaining frags
	ret = gf_gen_decode_matrix_simple(encode_matrix, decode_matrix,
					  invert_matrix, temp_matrix, decode_index,
					  frag_err_list, nerrs, k, m);
	
// 	printf("encode_matrix is \n");
// 	for(i=0;i<m*k;i+=k){
// 	    printf("%d ", encode_matrix[i]);
// 	    printf("\n");
// 	}
//    printf("decode_matrix is \n");
// 	for(i=0;i<m*k;i++){
// 	    printf("%d ",decode_matrix[i]);
// 	    printf("\n");
// 	}

	if (ret != 0) {
		printf("Fail on generate decode matrix\n");
		return -1;
	}

	// Pack recovery array pointers as list of valid fragments
	for (i = 0; i < k; i++)
		recover_srcs[i] = frag_ptrs[decode_index[i]];

	// Recover data
	ec_init_tables(k, nerrs, decode_matrix, g_tbls);
	ec_encode_data(len, k, nerrs, g_tbls, recover_srcs, recover_outp);

	// Check that recovered buffers are the same as original
	printf(" check recovery of block {");
	for (i = 0; i < nerrs; i++) {
		printf(" %d", frag_err_list[i]);
		if (memcmp(recover_outp[i], frag_ptrs[frag_err_list[i]], len)) {
			printf(" Fail erasure recovery %d, frag %d\n", i, frag_err_list[i]);
			return -1;
		}
	}
	clock_gettime(CLOCK_REALTIME, &time2);
	de_code = time2.tv_nsec-time1.tv_nsec;


	long sum = allocate_buffer + cauchy_matrix + en_init + en_code + de_code;
	printf("allocate_buffer is %ld\n", allocate_buffer);
	printf("cauchy_matrix is %ld\n", cauchy_matrix);
	printf("en_init is %ld\n", en_init);
	printf("en_code is %ld\n", en_code);
	printf("de_code is %ld\n", de_code);
	printf("sum time is %ld\n", sum);

}


int main(int argc, char *argv[])
{
	int all_len[12] = {16, 32, 64, 128, 256, 512, 1024, 1024*2, 1024*4, 1024*8, 1024*16, 1024*32};
	int k = 4, p = 2, len = 1024*1024/k, c;
	while ((c = getopt(argc, argv, "l:")) != -1) {
		switch (c) {
		case 'l':
			len = atoi(optarg) * 1024 / k; 
			break;
		}
	}

	int tmp = all_len[6];
	long sum_total = 0;
	printf("len is %d \n", tmp);
	int runtimes=10;
	// sum_total=MicroEC_encode_test(k, p, tmp*1024/k);
	for(int i=0;i<runtimes;i++){
		// sum_total=NaiveEC_encode_test(k, p, tmp*1024/k);
		sum_total+=MicroEC_encode_test(k, p, tmp*1024/k);		
	}
	printf("encoding time is %ld\n",sum_total/runtimes);
	return 0;
}

/*
 * Generate decode matrix from encode matrix and erasure list
 *
 */

static int gf_gen_decode_matrix_simple(u8 * encode_matrix,
				       u8 * decode_matrix,
				       u8 * invert_matrix,
				       u8 * temp_matrix,
				       u8 * decode_index, u8 * frag_err_list, int nerrs, int k,
				       int m)
{
	int i, j, p, r;
	int nsrcerrs = 0;
	u8 s, *b = temp_matrix;
	u8 frag_in_err[MMAX];

	memset(frag_in_err, 0, sizeof(frag_in_err));

	// Order the fragments in erasure for easier sorting
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)
			nsrcerrs++;
		frag_in_err[frag_err_list[i]] = 1;
	}

	// Construct b (matrix that encoded remaining frags) by removing erased rows
	for (i = 0, r = 0; i < k; i++, r++) {
		while (frag_in_err[r])
			r++;
		for (j = 0; j < k; j++)
			b[k * i + j] = encode_matrix[k * r + j];
		decode_index[i] = r;
	}

	// Invert matrix to get recovery matrix
	if (gf_invert_matrix(b, invert_matrix, k) < 0)
		return -1;

	// Get decode matrix with only wanted recovery rows
	for (i = 0; i < nerrs; i++) {
		if (frag_err_list[i] < k)	// A src err
			for (j = 0; j < k; j++)
				decode_matrix[k * i + j] =
				    invert_matrix[k * frag_err_list[i] + j];
	}

	// For non-src (parity) erasures need to multiply encode matrix * invert
	for (p = 0; p < nerrs; p++) {
		if (frag_err_list[p] >= k) {	// A parity err
			for (i = 0; i < k; i++) {
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
