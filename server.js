const express = require('express');
const sqlite3 = require('sqlite3').verbose();
const { open } = require('sqlite');
const config = require('./src/config');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(express.json());
app.use(cors());

// Conexão com o banco de dados
let db;
async function setupDatabase() {
    db = await open({
        filename: config.dbPath,
        driver: sqlite3.Database
    });
    console.log('Conexão com o banco de dados estabelecida');
}

// Rota principal para busca de dados com paginação
app.get('/api/cnpj', async (req, res) => {
    try {
        const {
            termo,
            page = 1,
            limit = 20,
            uf,
            situacao,
            cnae
        } = req.query;

        const offset = (page - 1) * limit;

        // Base da consulta
        let query = `
      SELECT
        e.cnpj_basico,
        e.razao_social,
        est.cnpj_completo,
        est.nome_fantasia,
        est.situacao_cadastral,
        est.cnae_principal,
        est.uf,
        est.municipio,
        est.logradouro,
        est.numero,
        est.complemento,
        est.bairro,
        est.cep,
        est.email,
        est.telefone1
      FROM empresas e
      JOIN estabelecimentos est ON e.cnpj_basico = est.cnpj_basico
    `;

        // Condições de busca
        const conditions = [];
        const params = [];

        if (termo) {
            conditions.push(`(
        est.cnpj_completo LIKE ? OR
        e.razao_social LIKE ? OR
        est.nome_fantasia LIKE ? OR
        EXISTS (SELECT 1 FROM socios s WHERE s.cnpj_basico = e.cnpj_basico AND s.nome_socio LIKE ?)
      )`);
            const termoBusca = `%${termo}%`;
            params.push(termoBusca, termoBusca, termoBusca, termoBusca);
        }

        if (uf) {
            conditions.push('est.uf = ?');
            params.push(uf);
        }

        if (situacao) {
            conditions.push('est.situacao_cadastral = ?');
            params.push(situacao);
        }

        if (cnae) {
            conditions.push('est.cnae_principal = ?');
            params.push(cnae);
        }

        if (conditions.length > 0) {
            query += ' WHERE ' + conditions.join(' AND ');
        }

        // Consulta para contagem total
        const countQuery = `SELECT COUNT(*) as total FROM (${query})`;
        const countResult = await db.get(countQuery, params);

        // Consulta principal com paginação
        query += ` ORDER BY e.razao_social LIMIT ? OFFSET ?`;
        params.push(parseInt(limit), parseInt(offset));

        const rows = await db.all(query, params);

        // Buscar sócios para cada empresa encontrada
        for (const row of rows) {
            const socios = await db.all(`
        SELECT nome_socio, qualificacao_socio, cnpj_cpf_socio
        FROM socios
        WHERE cnpj_basico = ?
      `, [row.cnpj_basico]);

            row.socios = socios;
        }

        res.json({
            total: countResult.total,
            page: parseInt(page),
            limit: parseInt(limit),
            totalPages: Math.ceil(countResult.total / limit),
            data: rows
        });
    } catch (error) {
        console.error('Erro na consulta:', error);
        res.status(500).json({ error: 'Erro ao buscar dados' });
    }
});

// Rota para obter detalhes de um CNPJ específico
app.get('/api/cnpj/:cnpj', async (req, res) => {
    try {
        const { cnpj } = req.params;

        // Remover caracteres não numéricos
        const cnpjLimpo = cnpj.replace(/\D/g, '');

        if (cnpjLimpo.length !== 14) {
            return res.status(400).json({ error: 'CNPJ inválido' });
        }

        const cnpjBasico = cnpjLimpo.substring(0, 8);

        // Buscar dados da empresa
        const empresa = await db.get(`
      SELECT * FROM empresas WHERE cnpj_basico = ?
    `, [cnpjBasico]);

        if (!empresa) {
            return res.status(404).json({ error: 'Empresa não encontrada' });
        }

        // Buscar estabelecimento
        const estabelecimento = await db.get(`
      SELECT * FROM estabelecimentos WHERE cnpj_completo = ?
    `, [cnpjLimpo]);

        // Buscar sócios
        const socios = await db.all(`
      SELECT * FROM socios WHERE cnpj_basico = ?
    `, [cnpjBasico]);

        // Buscar dados do Simples Nacional
        const simples = await db.get(`
      SELECT * FROM simples WHERE cnpj_basico = ?
    `, [cnpjBasico]);

        // Buscar descrição do CNAE principal
        let cnaeDescricao = null;
        if (estabelecimento && estabelecimento.cnae_principal) {
            const cnaeResult = await db.get(`
        SELECT descricao FROM cnae WHERE codigo = ?
      `, [estabelecimento.cnae_principal]);

            if (cnaeResult) {
                cnaeDescricao = cnaeResult.descricao;
            }
        }

        // Buscar descrição da natureza jurídica
        let naturezaJuridicaDescricao = null;
        if (empresa && empresa.natureza_juridica) {
            const naturezaResult = await db.get(`
        SELECT descricao FROM naturezas_juridicas WHERE codigo = ?
      `, [empresa.natureza_juridica]);

            if (naturezaResult) {
                naturezaJuridicaDescricao = naturezaResult.descricao;
            }
        }

        res.json({
            empresa,
            estabelecimento,
            socios,
            simples,
            cnae_descricao: cnaeDescricao,
            natureza_juridica_descricao: naturezaJuridicaDescricao
        });
    } catch (error) {
        console.error('Erro ao buscar detalhes do CNPJ:', error);
        res.status(500).json({ error: 'Erro ao buscar detalhes do CNPJ' });
    }
});

// Rota para obter dados de tabelas auxiliares (para filtros)
app.get('/api/auxiliares/:tabela', async (req, res) => {
    try {
        const { tabela } = req.params;

        const tabelasPermitidas = [
            'cnae', 'motivos', 'municipios', 'naturezas_juridicas',
            'paises', 'qualificacoes'
        ];

        if (!tabelasPermitidas.includes(tabela)) {
            return res.status(400).json({ error: 'Tabela não permitida' });
        }

        const rows = await db.all(`SELECT * FROM ${tabela}`);
        res.json(rows);
    } catch (error) {
        console.error(`Erro ao buscar dados da tabela ${req.params.tabela}:`, error);
        res.status(500).json({ error: 'Erro ao buscar dados auxiliares' });
    }
});

// Inicialização do servidor
async function startServer() {
    try {
        await setupDatabase();

        app.listen(PORT, () => {
            console.log(`Servidor rodando na porta ${PORT}`);
        });
    } catch (error) {
        console.error('Erro ao iniciar o servidor:', error);
    }
}

startServer();