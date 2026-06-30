//go:build amd64

#include "textflag.h"

// func adcAccumSSE(lut []float32, codes []byte) float32
//
// Processes 4 subspaces per iteration using SSE scalar-gather + vector-accumulate.
// Uses 4 independent scalar loads (pipelined by the CPU), packs into XMM,
// and accumulates with ADDPS. Tail handles remaining 1-3 subspaces.
//
// Stack frame (FP-relative):
//   lut:    0(FP) ptr, 8(FP) len, 16(FP) cap
//   codes: 24(FP) ptr, 32(FP) len, 40(FP) cap
//   ret:   48(FP) float32
TEXT ·adcAccumSSE(SB), NOSPLIT, $0-52
	// Load slice headers
	MOVQ lut_base+0(FP), SI      // SI = lut pointer (bytes)
	MOVQ codes_base+24(FP), DI   // DI = codes pointer
	MOVQ codes_len+32(FP), DX    // DX = NumSubSpaces

	// Zero X0 (4-lane float32 accumulator)
	XORPS X0, X0

	// R8 = loop counter (subspace index)
	XORQ R8, R8

	// R9 = NumSubSpaces rounded down to multiple of 4
	MOVQ DX, R9
	ANDQ $~3, R9

	// Main loop: process 4 subspaces per iteration
loop4:
	CMPQ R8, R9
	JGE  tail

	// Compute base byte offset for subspace R8: R8 * 1024
	// (Each subspace spans 256 float32s = 1024 bytes)
	MOVQ R8, R10
	SHLQ $10, R10              // R10 = R8 * 1024

	// Load 4 code bytes into R11
	MOVL (DI)(R8*1), R11

	// Extract code0 (low byte) and compute LUT byte address
	MOVL R11, R12
	ANDL $0xFF, R12             // code0
	// byte offset = R8*1024 + code0*4
	LEAQ (R10)(R12*4), R13      // R13 = byte offset within lut
	MOVSS (SI)(R13*1), X1       // X1[0] = lut[R8*256 + code0]

	// Extract code1 and compute LUT byte address
	MOVL R11, R12
	SHRL $8, R12
	ANDL $0xFF, R12             // code1
	LEAQ 1024(R10)(R12*4), R13  // R13 = (R8+1)*1024 + code1*4
	MOVSS (SI)(R13*1), X2       // X2[0] = lut[(R8+1)*256 + code1]

	// Extract code2 and compute LUT byte address
	MOVL R11, R12
	SHRL $16, R12
	ANDL $0xFF, R12             // code2
	LEAQ 2048(R10)(R12*4), R13  // R13 = (R8+2)*1024 + code2*4
	MOVSS (SI)(R13*1), X3       // X3[0] = lut[(R8+2)*256 + code2]

	// Extract code3 and compute LUT byte address
	MOVL R11, R12
	SHRL $24, R12
	ANDL $0xFF, R12             // code3
	LEAQ 3072(R10)(R12*4), R13  // R13 = (R8+3)*1024 + code3*4
	MOVSS (SI)(R13*1), X4       // X4[0] = lut[(R8+3)*256 + code3]

	// Pack X1, X2, X3, X4 into X1 = {v0, v1, v2, v3}
	UNPCKLPS X2, X1            // X1 = {v0, v1, 0, 0}
	UNPCKLPS X4, X3            // X3 = {v2, v3, 0, 0}
	SHUFPS   $0x44, X3, X1     // X1 = {v0, v1, v2, v3}

	// Accumulate: X0 += X1
	ADDPS X1, X0

	ADDQ $4, R8
	JMP  loop4

tail:
	CMPQ R8, DX
	JGE  done

	// Process remaining subspaces one at a time with scalar ADDSS
	MOVBLZX (DI)(R8*1), R12    // code byte (zero-extended)
	MOVQ    R8, R10
	SHLQ    $10, R10             // R10 = R8 * 1024
	LEAQ    (R10)(R12*4), R13   // byte offset
	MOVSS   (SI)(R13*1), X1
	ADDSS   X1, X0

	ADDQ $1, R8
	JMP  tail

done:
	// Horizontal sum of X0 = {s0, s1, s2, s3} → scalar
	MOVAPS   X0, X1
	SHUFPS   $0x0E, X1, X1     // X1 = {s2, s3, s0, s1}
	ADDPS    X1, X0             // X0 = {s0+s2, s1+s3, ...}
	MOVAPS   X0, X1
	SHUFPS   $0x01, X1, X1     // X1 = {s1+s3, s0+s2, ...}
	ADDSS    X1, X0             // X0[0] = total sum

	// Store result
	MOVSS X0, ret+48(FP)
	RET
