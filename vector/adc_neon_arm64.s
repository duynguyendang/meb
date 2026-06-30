//go:build arm64

#include "textflag.h"

// func adcAccumNEON(lut []float32, codes []byte) float32
//
// Processes 4 subspaces per iteration using 4 independent scalar accumulators
// (F0-F3) to expose instruction-level parallelism. Each accumulator handles
// one subspace, allowing the CPU to pipeline the independent loads and adds.
// Tail handles remaining 1-3 subspaces.
//
// Stack frame (FP-relative):
//   lut:    0(FP) ptr, 8(FP) len, 16(FP) cap
//   codes: 24(FP) ptr, 32(FP) len, 40(FP) cap
//   ret:   48(FP) float32
TEXT ·adcAccumNEON(SB), NOSPLIT, $0-52
	// Load slice headers
	MOVD lut_base+0(FP), R0      // R0 = lut pointer
	MOVD codes_base+24(FP), R1   // R1 = codes pointer
	MOVD codes_len+32(FP), R2    // R2 = NumSubSpaces

	// Zero 4 independent scalar accumulators
	FMOVS $0.0, F0
	FMOVS $0.0, F1
	FMOVS $0.0, F2
	FMOVS $0.0, F3

	// R3 = loop counter (subspace index)
	MOVD $0, R3

	// R4 = NumSubSpaces rounded down to multiple of 4
	MOVD R2, R4
	LSR  $2, R4, R4
	LSL  $2, R4, R4

	// Main loop: process 4 subspaces per iteration
loop4:
	CMP R4, R3
	BGE tail

	// Compute base byte offset for this group: R3 * 1024
	LSL $10, R3, R5            // R5 = R3 * 1024

	// Load 4 code bytes
	ADD R1, R3, R10            // R10 = codes + R3
	MOVBU (R10), R6            // code0
	MOVBU 1(R10), R7           // code1
	MOVBU 2(R10), R8           // code2
	MOVBU 3(R10), R9           // code3

	// Subspace 0: address = lut + R3*1024 + code0*4
	LSL $2, R6, R6             // code0 * 4
	ADD R5, R6, R6             // R3*1024 + code0*4
	ADD R0, R6, R11            // lut + offset
	FMOVS (R11), F4
	FADDS F4, F0, F0

	// Subspace 1: address = lut + R3*1024 + 1024 + code1*4
	LSL $2, R7, R7
	ADD R5, R7, R7
	ADD $1024, R7
	ADD R0, R7, R11
	FMOVS (R11), F4
	FADDS F4, F1, F1

	// Subspace 2: address = lut + R3*1024 + 2048 + code2*4
	LSL $2, R8, R8
	ADD R5, R8, R8
	ADD $2048, R8
	ADD R0, R8, R11
	FMOVS (R11), F4
	FADDS F4, F2, F2

	// Subspace 3: address = lut + R3*1024 + 3072 + code3*4
	LSL $2, R9, R9
	ADD R5, R9, R9
	ADD $3072, R9
	ADD R0, R9, R11
	FMOVS (R11), F4
	FADDS F4, F3, F3

	ADD $4, R3, R3
	B   loop4

tail:
	CMP R2, R3
	BGE done

	// Process remaining subspaces one at a time
	ADD R1, R3, R10            // R10 = codes + R3
	MOVBU (R10), R6            // code byte
	LSL   $10, R3, R5          // R3 * 1024
	LSL   $2, R6, R6           // code * 4
	ADD   R5, R6, R6           // R3*1024 + code*4
	ADD   R0, R6, R11          // lut + offset
	FMOVS (R11), F4
	FADDS F4, F0, F0

	ADD $1, R3, R3
	B   tail

done:
	// Sum the 4 accumulators: F0 = F0 + F1 + F2 + F3
	FADDS F1, F0, F0
	FADDS F2, F0, F0
	FADDS F3, F0, F0

	// Store result
	FMOVS F0, ret+48(FP)
	RET
