// Importando dependências
const config = require('./src/config');
const { downloadFile, unzipFile } = require('./src/utils/file');
const { createDatabase, createAdditionalIndices } = require('./src/database/schema');
const { processEmpresasFile } = require('./src/processors/empresas');
const { processEstabelecimentosFile } = require('./src/processors/estabelecimentos');

// Função principal
async function main() {
    try {
        console.log('Iniciando aplicação de importação de dados CNPJ para SQLite');

        // Criar banco de dados
        const db = await createDatabase();

        // Processar cada arquivo configurado
        for (const fileName of config.filesToDownload) {
            // Baixar arquivo
            const zipPath = await downloadFile(fileName);

            // Descompactar arquivo e obter lista de arquivos extraídos
            const extractedFiles = await unzipFile(zipPath);

            if (extractedFiles.length === 0) {
                console.error(`Nenhum arquivo foi extraído de ${zipPath}`);
                continue;
            }

            // Usar o primeiro arquivo extraído
            const finalFilePath = extractedFiles[0];
            console.log(`Usando arquivo extraído: ${finalFilePath}`);

            // Processar o arquivo conforme seu tipo
            if (fileName.startsWith('Empresas')) {
                await processEmpresasFile(db, finalFilePath);
            } else if (fileName.startsWith('Estabelecimentos')) {
                await processEstabelecimentosFile(db, finalFilePath);
            }
        }

        // Criar índices adicionais para melhorar performance
        await createAdditionalIndices(db);

        console.log('Fechando conexão com o banco de dados');
        await db.close();

        console.log('Processo concluído com sucesso!');
        console.log(`Banco de dados disponível em: ${config.dbPath}`);

    } catch (error) {
        console.error('Erro durante o processamento:', error);
        process.exit(1);
    }
}

// Executar a aplicação
main();