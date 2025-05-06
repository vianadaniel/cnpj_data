const fs = require('fs');
const path = require('path');
const axios = require('axios');
const unzipper = require('unzipper');
const { promisify } = require('util');
const stream = require('stream');
const pipeline = promisify(stream.pipeline);
const config = require('../config');

// Garantir que o diretório de download exista
if (!fs.existsSync(config.downloadDir)) {
    fs.mkdirSync(config.downloadDir, { recursive: true });
}

// Função para baixar um arquivo
async function downloadFile(fileName) {
    const fileUrl = `${config.baseUrl}/${fileName}.zip`;
    const outputPath = path.join(config.downloadDir, `${fileName}.zip`);

    console.log(`Baixando ${fileUrl}...`);

    const response = await axios({
        method: 'GET',
        url: fileUrl,
        responseType: 'stream',
    });

    await pipeline(
        response.data,
        fs.createWriteStream(outputPath)
    );

    console.log(`Download concluído: ${outputPath}`);
    return outputPath;
}

// Função para descompactar um arquivo zip e retornar o nome do arquivo extraído
async function unzipFile(zipPath) {
    const directory = path.dirname(zipPath);
    console.log(`Descompactando ${zipPath}...`);

    let extractedFiles = [];

    await new Promise((resolve, reject) => {
        fs.createReadStream(zipPath)
            .pipe(unzipper.Parse())
            .on('entry', async (entry) => {
                const fileName = entry.path;
                const outputPath = path.join(directory, fileName);
                extractedFiles.push(outputPath);

                try {
                    await pipeline(entry, fs.createWriteStream(outputPath));
                    console.log(`Arquivo extraído: ${outputPath}`);
                } catch (err) {
                    reject(err);
                }
            })
            .on('error', reject)
            .on('close', () => resolve());
    });

    console.log(`Descompactação concluída`);
    return extractedFiles;
}

module.exports = {
    downloadFile,
    unzipFile
};