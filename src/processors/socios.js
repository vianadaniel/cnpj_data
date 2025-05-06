const readline = require('readline');
const fs = require('fs');

/**
 * Processa um arquivo de Sócios e insere os dados no banco de dados
 * @param {Object} db - Conexão com o banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 */
async function processSociosFile(db, filePath) {
    console.log(`Iniciando processamento de Sócios: ${filePath}`);

    // Criar statement para inserção em lote
    const stmt = await db.prepare(`
        INSERT INTO socios (
            cnpj_basico,
            identificador_socio,
            nome_socio,
            cnpj_cpf_socio,
            qualificacao_socio,
            data_entrada_sociedade,
            pais,
            representante_legal,
            nome_representante,
            qualificacao_representante,
            faixa_etaria
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            // Formato esperado do arquivo de Sócios
            // CNPJ_BASICO;IDENTIFICADOR_SOCIO;NOME_SOCIO;CNPJ_CPF_SOCIO;QUALIFICACAO_SOCIO;DATA_ENTRADA_SOCIEDADE;PAIS;REPRESENTANTE_LEGAL;NOME_REPRESENTANTE;QUALIFICACAO_REPRESENTANTE;FAIXA_ETARIA
            const parts = line.split(';');

            if (parts.length >= 11) {
                const cnpjBasico = parts[0].trim();
                const identificadorSocio = parts[1].trim();
                const nomeSocio = parts[2].trim();
                const cnpjCpfSocio = parts[3].trim();
                const qualificacaoSocio = parts[4].trim();
                const dataEntradaSociedade = parts[5].trim() || null;
                const pais = parts[6].trim();
                const representanteLegal = parts[7].trim();
                const nomeRepresentante = parts[8].trim();
                const qualificacaoRepresentante = parts[9].trim();
                const faixaEtaria = parts[10].trim();

                try {
                    await stmt.run(
                        cnpjBasico,
                        identificadorSocio,
                        nomeSocio,
                        cnpjCpfSocio,
                        qualificacaoSocio,
                        dataEntradaSociedade,
                        pais,
                        representanteLegal,
                        nomeRepresentante,
                        qualificacaoRepresentante,
                        faixaEtaria
                    );
                    count++;

                    // Commit a cada 10000 registros para não sobrecarregar a memória
                    if (count % 10000 === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${count} registros de Sócios`);
                    }
                } catch (error) {
                    console.error(`Erro ao inserir Sócio ${nomeSocio} para CNPJ ${cnpjBasico}:`, error.message);
                }
            }
        }

        // Commit final
        await db.run('COMMIT');
        console.log(`Processamento de Sócios concluído. Total: ${count} registros`);

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
    processSociosFile
};