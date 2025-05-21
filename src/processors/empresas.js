const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de empresas e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 * @param {boolean} testMode - Se true, processa apenas a primeira linha
 */
async function processEmpresasFile(filePath, testMode = false) {
    const fileName = filePath.split('/').pop();
    console.log(`Iniciando processamento de empresas: ${fileName}`);
    if (testMode) {
        console.log(`[${fileName}] Modo de teste ativado - processando apenas uma linha`);
    }

    const client = await pool.connect();

    try {
        console.log(`[${fileName}] Iniciando processamento...`);
        let processed = 0;
        const BATCH_SIZE = 1000;
        let batch = [];

        // Criar stream de leitura com csv-parser
        const stream = fs.createReadStream(filePath, { encoding: 'latin1' })
            .pipe(csv({
                separator: ';',
                headers: ['cnpj_basico', 'razao_social', 'natureza_juridica', 'qualificacao_responsavel', 'capital_social', 'porte', 'ente_federativo'],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.cnpj_basico) {
                // Converter capital_social para número
                const capitalSocial = row.capital_social ?
                    parseFloat(row.capital_social.replace(',', '.')) :
                    null;

                batch.push([
                    row.cnpj_basico,
                    row.razao_social,
                    row.natureza_juridica,
                    row.qualificacao_responsavel,
                    capitalSocial,
                    row.porte,
                    row.ente_federativo
                ]);

                if (batch.length >= BATCH_SIZE || testMode) {
                    await client.query('BEGIN');
                    try {
                        // Criar tabela temporária para COPY
                        await client.query(`
                            CREATE TEMP TABLE temp_empresas (
                                cnpj_basico TEXT,
                                razao_social TEXT,
                                natureza_juridica TEXT,
                                qualificacao_responsavel TEXT,
                                capital_social NUMERIC,
                                porte TEXT,
                                ente_federativo TEXT
                            ) ON COMMIT DROP
                        `);

                        const values = [];
                        const placeholders = [];
                        let paramCount = 1;

                        for (const record of batch) {
                            values.push(...record);
                            placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6})`);
                            paramCount += 7;
                        }

                        await client.query(`
                            INSERT INTO temp_empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte, ente_federativo)
                            VALUES ${placeholders.join(', ')}
                        `, values);

                        // Inserir dados da tabela temporária na tabela final
                        await client.query(`
                            INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo)
                            SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte, ente_federativo FROM temp_empresas
                            ON CONFLICT (cnpj_basico) DO UPDATE
                            SET razao_social = EXCLUDED.razao_social,
                                natureza_juridica = EXCLUDED.natureza_juridica,
                                qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
                                capital_social = EXCLUDED.capital_social,
                                porte_empresa = EXCLUDED.porte_empresa,
                                ente_federativo = EXCLUDED.ente_federativo
                        `);

                        await client.query('COMMIT');

                        processed += batch.length;
                        console.log(`[${fileName}] Processados ${processed} registros de empresas`);

                        if (testMode) {
                            break; // Sai do loop após processar a primeira linha em modo de teste
                        }
                    } catch (error) {
                        await client.query('ROLLBACK');
                        console.error(`[${fileName}] Erro ao processar lote:`, error);
                        throw error;
                    }
                    batch = [];
                }
            }
        }

        // Processar registros restantes no batch
        if (batch.length > 0) {
            await client.query('BEGIN');
            try {
                // Criar tabela temporária para COPY
                await client.query(`
                    CREATE TEMP TABLE temp_empresas (
                        cnpj_basico TEXT,
                        razao_social TEXT,
                        natureza_juridica TEXT,
                        qualificacao_responsavel TEXT,
                        capital_social NUMERIC,
                        porte TEXT,
                        ente_federativo TEXT
                    ) ON COMMIT DROP
                `);

                const values = [];
                const placeholders = [];
                let paramCount = 1;

                for (const record of batch) {
                    values.push(...record);
                    placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6})`);
                    paramCount += 7;
                }

                await client.query(`
                    INSERT INTO temp_empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte, ente_federativo)
                    VALUES ${placeholders.join(', ')}
                `, values);

                // Inserir dados da tabela temporária na tabela final
                await client.query(`
                    INSERT INTO empresas (cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte_empresa, ente_federativo)
                    SELECT cnpj_basico, razao_social, natureza_juridica, qualificacao_responsavel, capital_social, porte, ente_federativo FROM temp_empresas
                    ON CONFLICT (cnpj_basico) DO UPDATE
                    SET razao_social = EXCLUDED.razao_social,
                        natureza_juridica = EXCLUDED.natureza_juridica,
                        qualificacao_responsavel = EXCLUDED.qualificacao_responsavel,
                        capital_social = EXCLUDED.capital_social,
                        porte_empresa = EXCLUDED.porte_empresa,
                        ente_federativo = EXCLUDED.ente_federativo
                `);

                await client.query('COMMIT');

                processed += batch.length;
                console.log(`[${fileName}] Processados registros finais: ${batch.length}`);
            } catch (error) {
                await client.query('ROLLBACK');
                console.error(`[${fileName}] Erro ao processar lote final:`, error);
                throw error;
            }
        }

        // Verificar dados na tabela final
        const finalCount = await client.query('SELECT COUNT(*) FROM empresas');
        console.log(`[${fileName}] Total de registros na tabela final: ${finalCount.rows[0].count}`);
        console.log(`[${fileName}] Processamento de empresas concluído. Total processado: ${processed} registros`);

    } catch (error) {
        console.error(`[${fileName}] Erro ao processar arquivo de empresas:`, error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processEmpresasFile
};