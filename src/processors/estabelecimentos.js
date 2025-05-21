const fs = require('fs');
const { pool } = require('../database/schema');
const csv = require('csv-parser');

/**
 * Processa um arquivo de estabelecimentos e insere os dados no banco de dados
 * @param {string} filePath - Caminho do arquivo a ser processado
 * @param {boolean} testMode - Se true, processa apenas a primeira linha
 */
async function processEstabelecimentosFile(filePath, testMode = false) {
    console.log(`Iniciando processamento de estabelecimentos: ${filePath}`);
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
                    'cnpj', 'cnpj_basico', 'matriz_filial', 'nome_fantasia', 'situacao_cadastral',
                    'data_situacao_cadastral', 'motivo_situacao_cadastral', 'nome_cidade_exterior',
                    'pais', 'data_inicio_atividade', 'cnae_fiscal', 'cnae_fiscal_secundaria',
                    'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro', 'cep',
                    'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax',
                    'fax', 'email', 'situacao_especial', 'data_situacao_especial'
                ],
                skipLines: 0
            }));

        // Processar cada linha do CSV
        for await (const row of stream) {
            if (row.cnpj) {
                // Extrair componentes do CNPJ
                const cnpjBasico = row.cnpj.substring(0, 8);
                const cnpjOrdem = row.cnpj.substring(8, 12);
                const cnpjDv = row.cnpj.substring(12, 14);

                batch.push([
                    cnpjBasico,
                    cnpjOrdem,
                    cnpjDv,
                    row.nome_fantasia,
                    row.situacao_cadastral,
                    row.data_situacao_cadastral,
                    row.motivo_situacao_cadastral,
                    row.nome_cidade_exterior,
                    row.pais,
                    row.data_inicio_atividade,
                    row.cnae_fiscal,
                    row.cnae_fiscal_secundaria,
                    row.tipo_logradouro,
                    row.logradouro,
                    row.numero,
                    row.complemento,
                    row.bairro,
                    row.cep,
                    row.uf,
                    row.municipio,
                    row.ddd_1,
                    row.telefone_1,
                    row.ddd_2,
                    row.telefone_2,
                    row.ddd_fax,
                    row.fax,
                    row.email,
                    row.situacao_especial,
                    row.data_situacao_especial
                ]);

                if (batch.length >= BATCH_SIZE || testMode) {
                    await client.query('BEGIN');
                    try {
                        // Criar tabela temporária para COPY
                        await client.query(`
                            CREATE TEMP TABLE temp_estabelecimentos (
                                cnpj_basico TEXT,
                                cnpj_ordem TEXT,
                                cnpj_dv TEXT,
                                nome_fantasia TEXT,
                                situacao_cadastral TEXT,
                                data_situacao_cadastral TEXT,
                                motivo_situacao_cadastral TEXT,
                                nome_cidade_exterior TEXT,
                                pais TEXT,
                                data_inicio_atividade TEXT,
                                cnae_fiscal TEXT,
                                cnae_fiscal_secundaria TEXT,
                                tipo_logradouro TEXT,
                                logradouro TEXT,
                                numero TEXT,
                                complemento TEXT,
                                bairro TEXT,
                                cep TEXT,
                                uf TEXT,
                                municipio TEXT,
                                ddd_1 TEXT,
                                telefone_1 TEXT,
                                ddd_2 TEXT,
                                telefone_2 TEXT,
                                ddd_fax TEXT,
                                fax TEXT,
                                email TEXT,
                                situacao_especial TEXT,
                                data_situacao_especial TEXT
                            ) ON COMMIT DROP
                        `);

                        const values = [];
                        const placeholders = [];
                        let paramCount = 1;

                        for (const record of batch) {
                            values.push(...record);
                            placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6}, $${paramCount + 7}, $${paramCount + 8}, $${paramCount + 9}, $${paramCount + 10}, $${paramCount + 11}, $${paramCount + 12}, $${paramCount + 13}, $${paramCount + 14}, $${paramCount + 15}, $${paramCount + 16}, $${paramCount + 17}, $${paramCount + 18}, $${paramCount + 19}, $${paramCount + 20}, $${paramCount + 21}, $${paramCount + 22}, $${paramCount + 23}, $${paramCount + 24}, $${paramCount + 25}, $${paramCount + 26}, $${paramCount + 27}, $${paramCount + 28})`);
                            paramCount += 29;
                        }

                        await client.query(`
                            INSERT INTO temp_estabelecimentos (
                                cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                                data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
                                pais, data_inicio_atividade, cnae_fiscal, cnae_fiscal_secundaria,
                                tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                                uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax,
                                fax, email, situacao_especial, data_situacao_especial
                            )
                            VALUES ${placeholders.join(', ')}
                        `, values);

                        // Inserir dados da tabela temporária na tabela final
                        await client.query(`
                            INSERT INTO estabelecimentos (
                                cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                                data_situacao_cadastral, motivo_situacao_cadastral, cidade_exterior,
                                pais, data_inicio_atividade, cnae_principal, cnaes_secundarios,
                                tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                                uf, municipio, ddd1, telefone1, ddd2, telefone2, email
                            )
                            SELECT
                                cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                                data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
                                pais, data_inicio_atividade, cnae_fiscal, cnae_fiscal_secundaria,
                                tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                                uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, email
                            FROM temp_estabelecimentos
                            ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE
                            SET nome_fantasia = EXCLUDED.nome_fantasia,
                                situacao_cadastral = EXCLUDED.situacao_cadastral,
                                data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
                                motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
                                cidade_exterior = EXCLUDED.cidade_exterior,
                                pais = EXCLUDED.pais,
                                data_inicio_atividade = EXCLUDED.data_inicio_atividade,
                                cnae_principal = EXCLUDED.cnae_principal,
                                cnaes_secundarios = EXCLUDED.cnaes_secundarios,
                                tipo_logradouro = EXCLUDED.tipo_logradouro,
                                logradouro = EXCLUDED.logradouro,
                                numero = EXCLUDED.numero,
                                complemento = EXCLUDED.complemento,
                                bairro = EXCLUDED.bairro,
                                cep = EXCLUDED.cep,
                                uf = EXCLUDED.uf,
                                municipio = EXCLUDED.municipio,
                                ddd1 = EXCLUDED.ddd1,
                                telefone1 = EXCLUDED.telefone1,
                                ddd2 = EXCLUDED.ddd2,
                                telefone2 = EXCLUDED.telefone2,
                                email = EXCLUDED.email
                        `);

                        await client.query('COMMIT');

                        processed += batch.length;
                        console.log(`Processados ${processed} registros de estabelecimentos`);

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
                    CREATE TEMP TABLE temp_estabelecimentos (
                        cnpj_basico TEXT,
                        cnpj_ordem TEXT,
                        cnpj_dv TEXT,
                        nome_fantasia TEXT,
                        situacao_cadastral TEXT,
                        data_situacao_cadastral TEXT,
                        motivo_situacao_cadastral TEXT,
                        nome_cidade_exterior TEXT,
                        pais TEXT,
                        data_inicio_atividade TEXT,
                        cnae_fiscal TEXT,
                        cnae_fiscal_secundaria TEXT,
                        tipo_logradouro TEXT,
                        logradouro TEXT,
                        numero TEXT,
                        complemento TEXT,
                        bairro TEXT,
                        cep TEXT,
                        uf TEXT,
                        municipio TEXT,
                        ddd_1 TEXT,
                        telefone_1 TEXT,
                        ddd_2 TEXT,
                        telefone_2 TEXT,
                        ddd_fax TEXT,
                        fax TEXT,
                        email TEXT,
                        situacao_especial TEXT,
                        data_situacao_especial TEXT
                    ) ON COMMIT DROP
                `);

                const values = [];
                const placeholders = [];
                let paramCount = 1;

                for (const record of batch) {
                    values.push(...record);
                    placeholders.push(`($${paramCount}, $${paramCount + 1}, $${paramCount + 2}, $${paramCount + 3}, $${paramCount + 4}, $${paramCount + 5}, $${paramCount + 6}, $${paramCount + 7}, $${paramCount + 8}, $${paramCount + 9}, $${paramCount + 10}, $${paramCount + 11}, $${paramCount + 12}, $${paramCount + 13}, $${paramCount + 14}, $${paramCount + 15}, $${paramCount + 16}, $${paramCount + 17}, $${paramCount + 18}, $${paramCount + 19}, $${paramCount + 20}, $${paramCount + 21}, $${paramCount + 22}, $${paramCount + 23}, $${paramCount + 24}, $${paramCount + 25}, $${paramCount + 26}, $${paramCount + 27}, $${paramCount + 28})`);
                    paramCount += 29;
                }

                await client.query(`
                    INSERT INTO temp_estabelecimentos (
                        cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                        data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
                        pais, data_inicio_atividade, cnae_fiscal, cnae_fiscal_secundaria,
                        tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                        uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, ddd_fax,
                        fax, email, situacao_especial, data_situacao_especial
                    )
                    VALUES ${placeholders.join(', ')}
                `, values);

                // Inserir dados da tabela temporária na tabela final
                await client.query(`
                    INSERT INTO estabelecimentos (
                        cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                        data_situacao_cadastral, motivo_situacao_cadastral, cidade_exterior,
                        pais, data_inicio_atividade, cnae_principal, cnaes_secundarios,
                        tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                        uf, municipio, ddd1, telefone1, ddd2, telefone2, email
                    )
                    SELECT
                        cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia, situacao_cadastral,
                        data_situacao_cadastral, motivo_situacao_cadastral, nome_cidade_exterior,
                        pais, data_inicio_atividade, cnae_fiscal, cnae_fiscal_secundaria,
                        tipo_logradouro, logradouro, numero, complemento, bairro, cep,
                        uf, municipio, ddd_1, telefone_1, ddd_2, telefone_2, email
                    FROM temp_estabelecimentos
                    ON CONFLICT (cnpj_basico, cnpj_ordem, cnpj_dv) DO UPDATE
                    SET nome_fantasia = EXCLUDED.nome_fantasia,
                        situacao_cadastral = EXCLUDED.situacao_cadastral,
                        data_situacao_cadastral = EXCLUDED.data_situacao_cadastral,
                        motivo_situacao_cadastral = EXCLUDED.motivo_situacao_cadastral,
                        cidade_exterior = EXCLUDED.cidade_exterior,
                        pais = EXCLUDED.pais,
                        data_inicio_atividade = EXCLUDED.data_inicio_atividade,
                        cnae_principal = EXCLUDED.cnae_principal,
                        cnaes_secundarios = EXCLUDED.cnaes_secundarios,
                        tipo_logradouro = EXCLUDED.tipo_logradouro,
                        logradouro = EXCLUDED.logradouro,
                        numero = EXCLUDED.numero,
                        complemento = EXCLUDED.complemento,
                        bairro = EXCLUDED.bairro,
                        cep = EXCLUDED.cep,
                        uf = EXCLUDED.uf,
                        municipio = EXCLUDED.municipio,
                        ddd1 = EXCLUDED.ddd1,
                        telefone1 = EXCLUDED.telefone1,
                        ddd2 = EXCLUDED.ddd2,
                        telefone2 = EXCLUDED.telefone2,
                        email = EXCLUDED.email
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
        const finalCount = await client.query('SELECT COUNT(*) FROM estabelecimentos');
        console.log(`Total de registros na tabela final: ${finalCount.rows[0].count}`);
        console.log(`Processamento de estabelecimentos concluído. Total processado: ${processed} registros`);

    } catch (error) {
        console.error('Erro ao processar arquivo de estabelecimentos:', error);
        throw error;
    } finally {
        client.release();
    }
}

module.exports = {
    processEstabelecimentosFile
};
