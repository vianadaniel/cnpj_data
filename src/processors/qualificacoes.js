const readline = require('readline');
const fs = require('fs');

/**
 * Processa um arquivo de Qualificações e insere os dados no banco de dados
 * @param {Object} db - Conexão com o banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processQualificacoesFile(db, filePath) {
    console.log(`Iniciando processamento de Qualificações: ${filePath}`);

    // Criar statement para inserção em lote
    const stmt = await db.prepare(`
        INSERT INTO qualificacoes (
            codigo,
            descricao
        ) VALUES (?, ?)
    `);

    // Iniciar transação para melhor performance
    await db.run('BEGIN TRANSACTION');

    const fileStream = fs.createReadStream(filePath);
    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let count = 0;

    try {
        for await (const line of rl) {
            // Formato esperado do arquivo de Qualificações
            // Código;Descrição
            const parts = line.split(';');

            if (parts.length >= 2) {
                const codigo = parts[0].trim();
                const descricao = parts[1].trim();

                try {
                    await stmt.run(codigo, descricao);
                    count++;

                    // Commit a cada 1000 registros
                    // (Qualificações geralmente tem poucos registros)
                    if (count % 1000 === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${count} registros de Qualificações`);
                    }
                } catch (error) {
                    console.error(`Erro ao inserir Qualificação ${codigo}:`, error.message);
                }
            }
        }

        // Commit final
        await db.run('COMMIT');
        console.log(`Processamento de Qualificações concluído. Total: ${count} registros`);

    } catch (error) {
        // Rollback em caso de erro
        await db.run('ROLLBACK');
        throw error;
    } finally {
        // Finalizar statement
        await stmt.finalize();
    }
}

module.exports = {
    processQualificacoesFile
};