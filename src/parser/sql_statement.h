/*
 * SQLStatement.h
 * Definition of the structure used to build the syntax tree.
 */

#pragma once

#include <vector>
#include <iostream>

#include "expr.h"

namespace nstore {
namespace parser {

typedef enum {
	kStmtError, // Unused
	kStmtSelect,
	kStmtImport,
	kStmtInsert,
	kStmtUpdate,
	kStmtDelete,
	kStmtCreate,
	kStmtDrop,
	kStmtPrepare,
	kStmtExecute,
	kStmtExport,
	kStmtRename,
	kStmtAlter
} StatementType;


/**
 * @struct SQLStatement
 * @brief Base class for every SQLStatement
 */
class SQLStatement {
 public:

  SQLStatement(StatementType type) :
		_type(type) {};

	virtual ~SQLStatement() {}

	virtual StatementType type() { return _type; }


  // Get a string representation of this statement
  friend std::ostream& operator<<(std::ostream& os, const SQLStatement& stmt);

private:
	StatementType _type;
};

/**
 * @struct SQLStatementList
 * @brief Represents the result of the SQLParser. If parsing was successful it is a list of SQLStatement.
 */
class SQLStatementList {
public:

	SQLStatementList() :
		isValid(true),
		parser_msg(NULL) {};

	SQLStatementList(SQLStatement* stmt) :
		isValid(true),
		parser_msg(NULL) {
		addStatement(stmt);	
	};
		
	virtual ~SQLStatementList() {

	  // clean up statements
	  for(auto stmt : statements)
	    delete stmt;

		delete parser_msg;
	}

	void addStatement(SQLStatement* stmt) { statements.push_back(stmt); }
	SQLStatement* getStatement(int id) { return statements[id]; }
	size_t numStatements() { return statements.size(); }

  // Get a string representation of this statement list
  friend std::ostream& operator<<(std::ostream& os, const SQLStatementList& stmt_list);

	std::vector<SQLStatement*> statements;
	bool isValid;
	const char* parser_msg;
	int error_line;
	int error_col;
};


} // End parser namespace
} // End nstore namespace

