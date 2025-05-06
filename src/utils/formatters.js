/**
 * Formata um código CNAE para o formato padrão
 * @param {string} cnae - Código CNAE a ser formatado
 * @returns {string} Código CNAE formatado
 */
function formatCnaeCode(cnae) {
    // Remove caracteres não numéricos
    const cleaned = cnae.replace(/\D/g, '');

    // Formata o código CNAE (geralmente no formato 0000-0/00)
    if (cleaned.length >= 7) {
        return `${cleaned.substring(0, 4)}-${cleaned.substring(4, 5)}/${cleaned.substring(5)}`;
    }

    return cleaned;
}

module.exports = {
    formatCnaeCode
};