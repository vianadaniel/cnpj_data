const readline = require('readline');
const fs = require('fs');

/**
 * Processa um arquivo de Simples Nacional e insere os dados no banco de dados
 * @param {Object} db - Conexão com o banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processSimplesFile(db, filePath) {
    console.log(`Iniciando processamento de Simples Nacional: ${filePath}`);

    // Criar statement para inserção em lote
    const stmt = await db.prepare(`
        INSERT INTO simples (
            cnpj_basico,
            opcao_simples,
            data_opcao_simples,
            data_exclusao_simples,
            opcao_mei,
            data_opcao_mei,
            data_exclusao_mei
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
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
            // Formato esperado do arquivo de Simples Nacional
            // CNPJ_BASICO;OPCAO_PELO_SIMPLES;DATA_OPCAO_SIMPLES;DATA_EXCLUSAO_SIMPLES;OPCAO_PELO_MEI;DATA_OPCAO_MEI;DATA_EXCLUSAO_MEI
            const parts = line.split(';');

            if (parts.length >= 7) {
                const cnpjBasico = parts[0].trim();
                const opcaoSimples = parts[1].trim() === 'S' ? 1 : 0;
                const dataOpcaoSimples = parts[2].trim() || null;
                const dataExclusaoSimples = parts[3].trim() || null;
                const opcaoMei = parts[4].trim() === 'S' ? 1 : 0;
                const dataOpcaoMei = parts[5].trim() || null;
                const dataExclusaoMei = parts[6].trim() || null;

                try {
                    await stmt.run(
                        cnpjBasico,
                        opcaoSimples,
                        dataOpcaoSimples,
                        dataExclusaoSimples,
                        opcaoMei,
                        dataOpcaoMei,
                        dataExclusaoMei
                    );
                    count++;

                    // Commit a cada 10000 registros para não sobrecarregar a memória
                    if (count % 10000 === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${count} registros de Simples Nacional`);
                    }
                } catch (error) {
                    console.error(`Erro ao inserir Simples Nacional para CNPJ ${cnpjBasico}:`, error.message);
                }
            }
        }

        // Commit final
        await db.run('COMMIT');
        console.log(`Processamento de Simples Nacional concluído. Total: ${count} registros`);

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
    processSimplesFile
};