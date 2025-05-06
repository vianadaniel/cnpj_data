const readline = require('readline');
const fs = require('fs');

/**
 * Processa um arquivo de Municípios e insere os dados no banco de dados
 * @param {Object} db - Conexão com o banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processMunicipiosFile(db, filePath) {
    console.log(`Iniciando processamento de Municípios: ${filePath}`);

    // Criar statement para inserção em lote
    const stmt = await db.prepare(`
        INSERT INTO municipios (
            codigo,
            nome,
            uf
        ) VALUES (?, ?, ?)
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
            // Formato esperado do arquivo de Municípios
            // Código;Nome;UF
            const parts = line.split(';');

            if (parts.length >= 3) {
                const codigo = parts[0].trim();
                const nome = parts[1].trim();
                const uf = parts[2].trim();

                try {
                    await stmt.run(codigo, nome, uf);
                    count++;

                    // Commit a cada 1000 registros
                    // (Municípios geralmente tem poucos registros, então podemos usar um valor menor)
                    if (count % 1000 === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${count} registros de Municípios`);
                    }
                } catch (error) {
                    console.error(`Erro ao inserir Município ${codigo} - ${nome}:`, error.message);
                }
            }
        }

        // Commit final
        await db.run('COMMIT');
        console.log(`Processamento de Municípios concluído. Total: ${count} registros`);

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
    processMunicipiosFile
};