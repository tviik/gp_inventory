/* ============================
   SQL PARSER
   ============================
   
   Парсер SQL-подобного DSL для запросов к данным Excel.
   Преобразует текстовый запрос в AST (Abstract Syntax Tree).
*/

// ============================
// TOKEN TYPES
// ============================

const TOKEN_TYPES = {
    KEYWORD: 'KEYWORD',
    IDENTIFIER: 'IDENTIFIER',
    STRING: 'STRING',
    NUMBER: 'NUMBER',
    OPERATOR: 'OPERATOR',
    PUNCTUATION: 'PUNCTUATION',
    EOF: 'EOF'
};

const KEYWORDS = [
    'SELECT', 'FROM', 'WHERE', 'ORDER', 'BY', 'GROUP', 'JOIN', 'ON', 'INNER', 'LEFT', 'RIGHT',
    'AND', 'OR', 'NOT', 'IN', 'LIKE', 'AS', 'ASC', 'DESC', 'LIMIT',
    'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'
];

// ============================
// LEXER
// ============================

class Lexer {
    constructor(input) {
        this.input = input.trim();
        this.position = 0;
        this.currentChar = this.input[this.position] || null;
    }

    advance() {
        this.position++;
        this.currentChar = this.position < this.input.length ? this.input[this.position] : null;
    }

    skipWhitespace() {
        while (this.currentChar && /\s/.test(this.currentChar)) {
            this.advance();
        }
    }

    readString() {
        const quote = this.currentChar;
        this.advance();
        let value = '';
        
        while (this.currentChar && this.currentChar !== quote) {
            if (this.currentChar === '\\') {
                this.advance();
                if (this.currentChar) {
                    value += this.currentChar;
                    this.advance();
                }
            } else {
                value += this.currentChar;
                this.advance();
            }
        }
        
        if (this.currentChar === quote) {
            this.advance();
        }
        
        return value;
    }

    readNumber() {
        let value = '';
        while (this.currentChar && /[\d.]/.test(this.currentChar)) {
            value += this.currentChar;
            this.advance();
        }
        return parseFloat(value);
    }

    readIdentifier() {
        let value = '';
        while (this.currentChar && /[\w.]/.test(this.currentChar)) {
            value += this.currentChar;
            this.advance();
        }
        return value;
    }

    readOperator() {
        let op = this.currentChar;
        this.advance();
        
        // Двухсимвольные операторы
        if (this.currentChar && ['=', '>', '<'].includes(op)) {
            const next = this.currentChar;
            if ((op === '<' && next === '>') || (op === '!' && next === '=') ||
                (op === '=' && next === '=') || (op === '>' && next === '=') ||
                (op === '<' && next === '=')) {
                op += next;
                this.advance();
            }
        }
        
        return op;
    }

    nextToken() {
        while (this.currentChar) {
            if (/\s/.test(this.currentChar)) {
                this.skipWhitespace();
                continue;
            }

            if (this.currentChar === '"' || this.currentChar === "'") {
                return {
                    type: TOKEN_TYPES.STRING,
                    value: this.readString()
                };
            }

            if (/\d/.test(this.currentChar)) {
                return {
                    type: TOKEN_TYPES.NUMBER,
                    value: this.readNumber()
                };
            }

            if (/[=<>!]/.test(this.currentChar)) {
                return {
                    type: TOKEN_TYPES.OPERATOR,
                    value: this.readOperator()
                };
            }

            if (/[(),;]/.test(this.currentChar)) {
                const char = this.currentChar;
                this.advance();
                return {
                    type: TOKEN_TYPES.PUNCTUATION,
                    value: char
                };
            }

            const identifier = this.readIdentifier();
            const upper = identifier.toUpperCase();
            
            if (KEYWORDS.includes(upper)) {
                return {
                    type: TOKEN_TYPES.KEYWORD,
                    value: upper
                };
            }

            return {
                type: TOKEN_TYPES.IDENTIFIER,
                value: identifier
            };
        }

        return {
            type: TOKEN_TYPES.EOF,
            value: null
        };
    }

    tokenize() {
        const tokens = [];
        let token = this.nextToken();
        
        while (token.type !== TOKEN_TYPES.EOF) {
            tokens.push(token);
            token = this.nextToken();
        }
        
        tokens.push(token); // EOF
        return tokens;
    }
}

// ============================
// PARSER
// ============================

class Parser {
    constructor(tokens) {
        this.tokens = tokens;
        this.position = 0;
        this.currentToken = this.tokens[this.position];
    }

    advance() {
        this.position++;
        this.currentToken = this.position < this.tokens.length 
            ? this.tokens[this.position] 
            : { type: TOKEN_TYPES.EOF, value: null };
    }

    expect(type, value = null) {
        if (this.currentToken.type !== type) {
            throw new Error(`Expected ${type}, got ${this.currentToken.type}`);
        }
        if (value !== null && this.currentToken.value !== value) {
            throw new Error(`Expected ${value}, got ${this.currentToken.value}`);
        }
        const token = this.currentToken;
        this.advance();
        return token;
    }

    parseSelect() {
        this.expect(TOKEN_TYPES.KEYWORD, 'SELECT');
        
        const columns = [];
        
        if (this.currentToken.value === '*') {
            columns.push('*');
            this.advance();
        } else {
            while (true) {
                const column = this.parseColumn();
                columns.push(column);
                
                if (this.currentToken.value === ',') {
                    this.advance();
                } else {
                    break;
                }
            }
        }
        
        return columns;
    }

    parseColumn() {
        // Проверяем, является ли это агрегацией (COUNT, SUM, etc.)
        const token = this.currentToken;
        if (token.type === TOKEN_TYPES.KEYWORD && 
            ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX'].includes(token.value)) {
            const aggType = token.value;
            this.advance();
            this.expect(TOKEN_TYPES.PUNCTUATION, '(');
            
            let column = null;
            if (this.currentToken.value === '*') {
                this.advance();
                column = '*';
            } else {
                column = this.expect(TOKEN_TYPES.IDENTIFIER).value;
            }
            
            this.expect(TOKEN_TYPES.PUNCTUATION, ')');
            
            const result = { type: aggType, column: column };
            
            // Alias
            if (this.currentToken.value === 'AS') {
                this.advance();
                result.alias = this.expect(TOKEN_TYPES.IDENTIFIER).value;
            }
            
            return result;
        }
        
        // Обычная колонка
        let column = this.expect(TOKEN_TYPES.IDENTIFIER).value;
        
        // Поддержка table.column
        if (this.currentToken.value === '.') {
            this.advance();
            column += '.' + this.expect(TOKEN_TYPES.IDENTIFIER).value;
        }
        
        // Alias
        if (this.currentToken.value === 'AS') {
            this.advance();
            const alias = this.expect(TOKEN_TYPES.IDENTIFIER).value;
            return { column: column, alias: alias };
        }
        
        return column;
    }

    parseFrom() {
        if (this.currentToken.value === 'FROM') {
            this.advance();
            return this.expect(TOKEN_TYPES.IDENTIFIER).value;
        }
        return null;
    }

    parseWhere() {
        if (this.currentToken.value !== 'WHERE') {
            return null;
        }
        
        this.advance();
        return this.parseCondition();
    }

    parseCondition() {
        let left = this.parseConditionTerm();
        
        while (this.currentToken.value === 'AND' || this.currentToken.value === 'OR') {
            const op = this.currentToken.value;
            this.advance();
            const right = this.parseConditionTerm();
            left = {
                type: op,
                left: left,
                right: right
            };
        }
        
        return left;
    }

    parseConditionTerm() {
        if (this.currentToken.value === 'NOT') {
            this.advance();
            return {
                type: 'NOT',
                condition: this.parseConditionTerm()
            };
        }
        
        if (this.currentToken.value === '(') {
            this.advance();
            const condition = this.parseCondition();
            this.expect(TOKEN_TYPES.PUNCTUATION, ')');
            return condition;
        }
        
        const column = this.parseColumnIdentifier();
        const operator = this.expect(TOKEN_TYPES.OPERATOR).value;
        let value = this.parseValue();
        
        // Специальная обработка IN
        if (operator === 'IN' && this.currentToken.value === '(') {
            this.advance();
            const values = [];
            while (this.currentToken.value !== ')') {
                values.push(this.parseValue());
                if (this.currentToken.value === ',') {
                    this.advance();
                }
            }
            this.expect(TOKEN_TYPES.PUNCTUATION, ')');
            value = values;
        }
        
        return {
            type: operator,
            column: column,
            value: value
        };
    }

    parseColumnIdentifier() {
        let column = this.expect(TOKEN_TYPES.IDENTIFIER).value;
        
        if (this.currentToken.value === '.') {
            this.advance();
            column += '.' + this.expect(TOKEN_TYPES.IDENTIFIER).value;
        }
        
        return column;
    }

    parseValue() {
        if (this.currentToken.type === TOKEN_TYPES.STRING) {
            const value = this.currentToken.value;
            this.advance();
            return value;
        }
        
        if (this.currentToken.type === TOKEN_TYPES.NUMBER) {
            const value = this.currentToken.value;
            this.advance();
            return value;
        }
        
        if (this.currentToken.type === TOKEN_TYPES.IDENTIFIER) {
            const value = this.currentToken.value;
            this.advance();
            return value;
        }
        
        throw new Error(`Unexpected token in value: ${this.currentToken.type}`);
    }

    parseOrderBy() {
        if (this.currentToken.value !== 'ORDER') {
            return null;
        }
        
        this.expect(TOKEN_TYPES.KEYWORD, 'ORDER');
        this.expect(TOKEN_TYPES.KEYWORD, 'BY');
        
        const orders = [];
        
        while (true) {
            const column = this.parseColumnIdentifier();
            let direction = 'ASC';
            
            if (this.currentToken.value === 'ASC' || this.currentToken.value === 'DESC') {
                direction = this.currentToken.value;
                this.advance();
            }
            
            orders.push({ column, direction });
            
            if (this.currentToken.value === ',') {
                this.advance();
            } else {
                break;
            }
        }
        
        return orders;
    }

    parseGroupBy() {
        if (this.currentToken.value !== 'GROUP') {
            return null;
        }
        
        this.expect(TOKEN_TYPES.KEYWORD, 'GROUP');
        this.expect(TOKEN_TYPES.KEYWORD, 'BY');
        
        const columns = [];
        
        while (true) {
            columns.push(this.parseColumnIdentifier());
            
            if (this.currentToken.value === ',') {
                this.advance();
            } else {
                break;
            }
        }
        
        return columns;
    }

    parseJoin() {
        if (this.currentToken.value !== 'JOIN' && 
            this.currentToken.value !== 'INNER' && 
            this.currentToken.value !== 'LEFT' &&
            this.currentToken.value !== 'RIGHT') {
            return null;
        }
        
        let joinType = 'INNER';
        
        if (this.currentToken.value === 'INNER' || 
            this.currentToken.value === 'LEFT' ||
            this.currentToken.value === 'RIGHT') {
            joinType = this.currentToken.value;
            this.advance();
        }
        
        this.expect(TOKEN_TYPES.KEYWORD, 'JOIN');
        const table = this.expect(TOKEN_TYPES.IDENTIFIER).value;
        this.expect(TOKEN_TYPES.KEYWORD, 'ON');
        
        const left = this.parseColumnIdentifier();
        this.expect(TOKEN_TYPES.OPERATOR, '=');
        const right = this.parseColumnIdentifier();
        
        return {
            type: joinType,
            table: table,
            on: {
                left: left,
                right: right
            }
        };
    }

    parseLimit() {
        if (this.currentToken.value !== 'LIMIT') {
            return null;
        }
        
        this.advance();
        const limit = this.expect(TOKEN_TYPES.NUMBER).value;
        return limit;
    }

    parse() {
        const ast = {
            type: 'SELECT',
            columns: this.parseSelect(),
            from: this.parseFrom(),
            where: this.parseWhere(),
            orderBy: this.parseOrderBy(),
            groupBy: this.parseGroupBy(),
            join: this.parseJoin(),
            limit: this.parseLimit()
        };
        
        return ast;
    }
}

// ============================
// PUBLIC API
// ============================

/**
 * Парсинг SQL-запроса в AST
 * @param {string} query - SQL-запрос
 * @returns {Object} AST
 */
export function parseQuery(query) {
    try {
        const lexer = new Lexer(query);
        const tokens = lexer.tokenize();
        const parser = new Parser(tokens);
        return parser.parse();
    } catch (error) {
        throw new Error(`Parse error: ${error.message}`);
    }
}

export default { parseQuery };

