// Importando dependências
const config = require('./src/config');
const { downloadFile, unzipFile } = require('./src/utils/file');
const fs = require('fs').promises;
const path = require('path');

// Função principal para download e extração
async function downloadAndExtract() {
    try {
        console.log('Iniciando download e extração de dados CNPJ');

        // Criar diretório para armazenar informações sobre arquivos extraídos
        const extractInfoDir = path.join(__dirname, 'extracted_files_info');
        try {
            await fs.mkdir(extractInfoDir, { recursive: true });
        } catch (err) {
            if (err.code !== 'EEXIST') throw err;
        }

        const extractedFilesInfo = {};

        // Processar cada arquivo configurado
        for (const fileName of config.filesToDownload) {
            console.log(`Baixando ${fileName}...`);
            // Baixar arquivo
            const zipPath = await downloadFile(fileName);

            console.log(`Extraindo ${zipPath}...`);
            // Descompactar arquivo e obter lista de arquivos extraídos
            const extractedFiles = await unzipFile(zipPath);

            if (extractedFiles.length === 0) {
                console.error(`Nenhum arquivo foi extraído de ${zipPath}`);
                continue;
            }

            // Armazenar informações sobre os arquivos extraídos
            extractedFilesInfo[fileName] = extractedFiles;
            console.log(`Extraído com sucesso: ${extractedFiles.join(', ')}`);
        }

        // Salvar informações sobre os arquivos extraídos para uso posterior
        const infoFilePath = path.join(extractInfoDir, 'extracted_files.json');
        await fs.writeFile(infoFilePath, JSON.stringify(extractedFilesInfo, null, 2));

        console.log('Download e extração concluídos com sucesso!');
        console.log(`Informações sobre arquivos extraídos salvas em: ${infoFilePath}`);

    } catch (error) {
        console.error('Erro durante o download e extração:', error);
        process.exit(1);
    }
}

// Executar o download e extração
downloadAndExtract();