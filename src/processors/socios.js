const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de sócios e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 * @param {boolean} testMode - Se true, processa apenas a primeira linha
 */
async function processSociosFile(filePath, testMode = false) {
    console.log(`Iniciando processamento de sócios: ${filePath}`);
    if (testMode) {
        console.log('Modo de teste ativado - processando apenas uma linha');
    }

    const client = await pool.connect();

    try {
        console.log('Iniciando processamento...');
        let processed = 0;
        const BATCH_SIZE = 1000;
        let batch = [];

        // Criar stream de leitura com csv-parser
        const stream = fs.createReadStream(filePath, { encoding: 'latin1' })
            .pipe(csv({
                separator: ';',
                headers: [
                    'cnpj', 'identificador_socio', 'nome_socio', 'cpf_cnpj_socio',
                    'qualificacao_socio', 'data_entrada_sociedade', 'pais',
                    'representante_legal', 'nome_representante', 'qualificacao_representante',
                    'faixa_etaria'
                ],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.cnpj) {
                batch.push([
                    row.cnpj,
                    row.identificador_socio,
                    row.nome_socio,
                    row.cpf_cnpj_socio,
                    row.qualificacao_socio,
                    row.data_entrada_sociedade,
                    row.pais,
                    row.representante_legal,
                    row.nome_representante,
                    row.qualificacao_representante,
                    row.faixa_etaria
                ]);

                if (batch.length >= BATCH_SIZE || testMode) {
                    await client.query('BEGIN');
                    try {
                        // Criar tabela temporária para COPY
                        await client.query(`
                            CREATE TEMP TABLE temp_socios (
                                cnpj TEXT,
                                identificador_socio TEXT,
                                nome_socio TEXT,
                                cpf_cnpj_socio TEXT,
                                qualificacao_socio TEXT,
                                data_entrada_sociedade TEXT,
                                pais TEXT,
                                representante_legal TEXT,
                                nome_representante TEXT,
                                qualificacao_representante TEXT,
                                faixa_etaria TEXT
                            ) ON COMMIT DROP
                        `);

                        const values = [];
                        const placeholders = [];
                        let paramCount = 1;

                        for (const record of batch) {
                            values.push(...record);
                            placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6}, $${paramCount + 7}, $${paramCount + 8}, $${paramCount + 9}, $${paramCount + 10})`);
                            paramCount += 11;
                        }

                        await client.query(`
                            INSERT INTO temp_socios (
                                cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                                qualificacao_socio, data_entrada_sociedade, pais,
                                representante_legal, nome_representante, qualificacao_representante,
                                faixa_etaria
                            )
                            VALUES ${placeholders.join(', ')}
                        `, values);

                        // Inserir dados da tabela temporária na tabela final
                        await client.query(`
                            INSERT INTO socios (
                                cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                                qualificacao_socio, data_entrada_sociedade, pais,
                                representante_legal, nome_representante, qualificacao_representante,
                                faixa_etaria
                            )
                            SELECT
                                cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                                qualificacao_socio, data_entrada_sociedade, pais,
                                representante_legal, nome_representante, qualificacao_representante,
                                faixa_etaria
                            FROM temp_socios
                            ON CONFLICT (cnpj, cpf_cnpj_socio) DO UPDATE
                            SET identificador_socio = EXCLUDED.identificador_socio,
                                nome_socio = EXCLUDED.nome_socio,
                                qualificacao_socio = EXCLUDED.qualificacao_socio,
                                data_entrada_sociedade = EXCLUDED.data_entrada_sociedade,
                                pais = EXCLUDED.pais,
                                representante_legal = EXCLUDED.representante_legal,
                                nome_representante = EXCLUDED.nome_representante,
                                qualificacao_representante = EXCLUDED.qualificacao_representante,
                                faixa_etaria = EXCLUDED.faixa_etaria
                        `);

                        await client.query('COMMIT');

                        processed += batch.length;
                        console.log(`Processados ${processed} registros de sócios`);

                        if (testMode) {
                            break; // Sai do loop após processar a primeira linha em modo de teste
                        }
                    } catch (error) {
                        await client.query('ROLLBACK');
                        console.error('Erro ao processar lote:', error);
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
                    CREATE TEMP TABLE temp_socios (
                        cnpj TEXT,
                        identificador_socio TEXT,
                        nome_socio TEXT,
                        cpf_cnpj_socio TEXT,
                        qualificacao_socio TEXT,
                        data_entrada_sociedade TEXT,
                        pais TEXT,
                        representante_legal TEXT,
                        nome_representante TEXT,
                        qualificacao_representante TEXT,
                        faixa_etaria TEXT
                    ) ON COMMIT DROP
                `);

                const values = [];
                const placeholders = [];
                let paramCount = 1;

                for (const record of batch) {
                    values.push(...record);
                    placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6}, $${paramCount + 7}, $${paramCount + 8}, $${paramCount + 9}, $${paramCount + 10})`);
                    paramCount += 11;
                }

                await client.query(`
                    INSERT INTO temp_socios (
                        cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                        qualificacao_socio, data_entrada_sociedade, pais,
                        representante_legal, nome_representante, qualificacao_representante,
                        faixa_etaria
                    )
                    VALUES ${placeholders.join(', ')}
                `, values);

                // Inserir dados da tabela temporária na tabela final
                await client.query(`
                    INSERT INTO socios (
                        cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                        qualificacao_socio, data_entrada_sociedade, pais,
                        representante_legal, nome_representante, qualificacao_representante,
                        faixa_etaria
                    )
                    SELECT
                        cnpj, identificador_socio, nome_socio, cpf_cnpj_socio,
                        qualificacao_socio, data_entrada_sociedade, pais,
                        representante_legal, nome_representante, qualificacao_representante,
                        faixa_etaria
                    FROM temp_socios
                    ON CONFLICT (cnpj, cpf_cnpj_socio) DO UPDATE
                    SET identificador_socio = EXCLUDED.identificador_socio,
                        nome_socio = EXCLUDED.nome_socio,
                        qualificacao_socio = EXCLUDED.qualificacao_socio,
                        data_entrada_sociedade = EXCLUDED.data_entrada_sociedade,
                        pais = EXCLUDED.pais,
                        representante_legal = EXCLUDED.representante_legal,
                        nome_representante = EXCLUDED.nome_representante,
                        qualificacao_representante = EXCLUDED.qualificacao_representante,
                        faixa_etaria = EXCLUDED.faixa_etaria
                `);

                await client.query('COMMIT');

                processed += batch.length;
                console.log(`Processados registros finais: ${batch.length}`);
            } catch (error) {
                await client.query('ROLLBACK');
                console.error('Erro ao processar lote final:', error);
                throw error;
            }
        }

        // Verificar dados na tabela final
        const finalCount = await client.query('SELECT COUNT(*) FROM socios');
        console.log(`Total de registros na tabela final: ${finalCount.rows[0].count}`);
        console.log(`Processamento de sócios concluído. Total processado: ${processed} registros`);

    } catch (error) {
        console.error('Erro ao processar arquivo de sócios:', error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processSociosFile
};