package predicates

// Predicate constants for structural and semantic relationships.
// These match the CSD Section 3.3 Predicate Constants.
const (
	// Structural Predicates
	PredDefines       = "defines"         // Type/function definition
	PredCalls         = "calls"           // Function invocation
	PredImports       = "imports"         // Package import
	PredImplements    = "implements"      // Interface implementation
	PredHasMethod     = "has_method"      // Method of type
	PredHasField      = "has_field"       // Field of struct
	PredReturns       = "returns"         // Return type
	PredAccepts       = "accepts"         // Parameter type
	PredTypeOf        = "type_of"         // Variable type
	PredDependsOn     = "depends_on"      // Dependency relationship
	PredHasDoc        = "has_doc"         // Documentation association
	PredHasSourceCode = "has_source_code" // Raw source code
	PredInPackage     = "in_package"      // Package membership
	PredHasTag        = "has_tag"         // Architectural tag
	PredHash          = "hash_sha256"     // Content hash

	// Semantic Classification Predicates
	PredType    = "type"     // Categorization (interface, struct, func, etc.)
	PredHasRole = "has_role" // Universal architectural roles
	PredName    = "name"     // Human-readable identifier

	// Cross-Layer Predicates
	PredCallsAPI  = "calls_api"  // Frontend → Virtual URI
	PredHandledBy = "handled_by" // Virtual URI → Backend Handler

	// Virtual Predicates (inferred)
	PotentiallyCalls = "v:potentially_calls"
	WiresTo          = "v:wires_to"
)

// Universal Roles for PredHasRole predicate
// These represent common architectural patterns
const (
	RoleEntryPoint = "entry_point" // Application entry points (main, handlers)
	RoleDataModel  = "data_model"  // Data structures and models
	RoleUtility    = "utility"     // Helper functions and utilities
	RoleService    = "service"     // Business logic services
	RoleMiddleware = "middleware"  // Cross-cutting concerns
)

// Type values for PredType predicate
const (
	TypeInterface = "interface"
	TypeStruct    = "struct"
	TypeFunction  = "function"
	TypeMethod    = "method"
	TypeVariable  = "variable"
	TypeConstant  = "constant"
	TypePackage   = "package"
)
