const fs = require('fs');
const csv = require('csv-parser');

async function processEstabelecimentosFile(db, filePath) {
    console.log(`Processando arquivo de estabelecimentos: ${filePath}`);

    try {
        await db.run('PRAGMA synchronous = NORMAL');
        await db.run('PRAGMA journal_mode = WAL');
        await db.run('PRAGMA temp_store = MEMORY');
        await db.run('PRAGMA cache_size = 10000');

        const stmt = await db.prepare(`
        INSERT OR REPLACE INTO estabelecimentos (
            cnpj_basico, cnpj_ordem, cnpj_dv, nome_fantasia,
            situacao_cadastral, data_situacao_cadastral, motivo_situacao_cadastral,
            cidade_exterior, pais, data_inicio_atividade, cnae_principal,
            cnaes_secundarios, tipo_logradouro, logradouro, numero, complemento,
            bairro, cep, uf, municipio, ddd1, telefone1, ddd2, telefone2, email
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `);

        const batchSize = 5000000;
        let processed = 0;

        await db.run('BEGIN TRANSACTION');

        await new Promise((resolve, reject) => {
            const stream = fs.createReadStream(filePath, { encoding: 'latin1' })
                .pipe(csv({
                    separator: ';',
                    headers: [
                        'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial', 'nome_fantasia',
                        'situacao_cadastral', 'data_situacao_cadastral', 'motivo_situacao_cadastral',
                        'nome_cidade_exterior', 'pais', 'data_inicio_atividade', 'cnae_fiscal_principal',
                        'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero', 'complemento',
                        'bairro', 'cep', 'uf', 'municipio', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2',
                        'ddd_fax', 'fax', 'email', 'situacao_especial', 'data_situacao_especial'
                    ],
                    skipLines: 0
                }));

            stream.on('data', async (row) => {
                try {
                    stream.pause();

                    await stmt.run(
                        row.cnpj_basico,
                        row.cnpj_ordem,
                        row.cnpj_dv,
                        row.nome_fantasia,
                        row.situacao_cadastral,
                        row.data_situacao_cadastral,
                        row.motivo_situacao_cadastral,
                        row.nome_cidade_exterior,
                        row.pais,
                        row.data_inicio_atividade,
                        row.cnae_fiscal_principal,
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
                        row.email
                    );

                    processed++;

                    if (processed % batchSize === 0) {
                        await db.run('COMMIT');
                        await db.run('BEGIN TRANSACTION');
                        console.log(`Processados ${processed} registros de estabelecimentos`);
                        await db.run('PRAGMA wal_checkpoint(PASSIVE)');
                    }

                    stream.resume();
                } catch (err) {
                    stream.destroy(err);
                    reject(err);
                }
            });

            stream.on('end', async () => {
                try {
                    await db.run('COMMIT');
                    console.log(`Processamento concluído. Total de ${processed} registros.`);
                    await stmt.finalize();
                    resolve();
                } catch (err) {
                    reject(err);
                }
            });

            stream.on('error', (err) => {
                reject(err);
            });
        });
    } catch (error) {
        console.error(`Erro ao processar arquivo de estabelecimentos: ${error.message}`);
        try {
            await db.run('ROLLBACK');
        } catch (rollbackError) {
            console.error(`Erro ao fazer rollback: ${rollbackError.message}`);
        }
        throw error;
    }
}

module.exports = {
    processEstabelecimentosFile
};
