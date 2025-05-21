const express = require('express');
const { Pool } = require('pg');
const config = require('./src/config');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3003;

// Middleware
app.use(express.json());
app.use(cors());

// Conexão com o banco de dados
const pool = new Pool({
    user: config.dbUser,
    host: config.dbHost,
    database: config.dbName,
    password: config.dbPassword,
    port: config.dbPort,
});

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
        est.cnpj_completo ILIKE $${params.length + 1} OR
        e.razao_social ILIKE $${params.length + 2} OR
        est.nome_fantasia ILIKE $${params.length + 3} OR
        EXISTS (SELECT 1 FROM socios s WHERE s.cnpj_basico = e.cnpj_basico AND s.nome_socio ILIKE $${params.length + 4})
      )`);
            const termoBusca = `%${termo}%`;
            params.push(termoBusca, termoBusca, termoBusca, termoBusca);
        }

        if (uf) {
            conditions.push(`est.uf = $${params.length + 1}`);
            params.push(uf);
        }

        if (situacao) {
            conditions.push(`est.situacao_cadastral = $${params.length + 1}`);
            params.push(situacao);
        }

        if (cnae) {
            conditions.push(`est.cnae_principal = $${params.length + 1}`);
            params.push(cnae);
        }

        if (conditions.length > 0) {
            query += ' WHERE ' + conditions.join(' AND ');
        }

        // Consulta para contagem total
        const countQuery = `SELECT COUNT(*) as total FROM (${query})`;
        const countResult = await pool.query(countQuery, params);

        // Consulta principal com paginação
        query += ` ORDER BY e.razao_social LIMIT $${params.length + 1} OFFSET $${params.length + 2}`;
        params.push(limit, offset);

        const result = await pool.query(query, params);

        res.json({
            total: parseInt(countResult.rows[0].total),
            page: parseInt(page),
            limit: parseInt(limit),
            data: result.rows
        });
    } catch (error) {
        console.error('Erro na busca:', error);
        res.status(500).json({ error: 'Erro interno do servidor' });
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
        const empresa = await pool.query(`
      SELECT * FROM empresas WHERE cnpj_basico = $1
    `, [cnpjBasico]);

        if (empresa.rows.length === 0) {
            return res.status(404).json({ error: 'Empresa não encontrada' });
        }

        // Buscar estabelecimento
        const estabelecimento = await pool.query(`
      SELECT * FROM estabelecimentos WHERE cnpj_completo = $1
    `, [cnpjLimpo]);

        // Buscar sócios
        const socios = await pool.query(`
      SELECT * FROM socios WHERE cnpj_basico = $1
    `, [cnpjBasico]);

        // Buscar dados do Simples Nacional
        const simples = await pool.query(`
      SELECT * FROM simples WHERE cnpj_basico = $1
    `, [cnpjBasico]);

        // Buscar descrição do CNAE principal
        let cnaeDescricao = null;
        if (estabelecimento.rows.length > 0 && estabelecimento.rows[0].cnae_principal) {
            const cnaeResult = await pool.query(`
        SELECT descricao FROM cnae WHERE codigo = $1
      `, [estabelecimento.rows[0].cnae_principal]);

            if (cnaeResult.rows.length > 0) {
                cnaeDescricao = cnaeResult.rows[0].descricao;
            }
        }

        // Buscar descrição da natureza jurídica
        let naturezaJuridicaDescricao = null;
        if (empresa.rows.length > 0 && empresa.rows[0].natureza_juridica) {
            const naturezaResult = await pool.query(`
        SELECT descricao FROM naturezas_juridicas WHERE codigo = $1
      `, [empresa.rows[0].natureza_juridica]);

            if (naturezaResult.rows.length > 0) {
                naturezaJuridicaDescricao = naturezaResult.rows[0].descricao;
            }
        }

        res.json({
            empresa: empresa.rows[0],
            estabelecimento: estabelecimento.rows[0],
            socios: socios.rows,
            simples: simples.rows[0],
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

        const rows = await pool.query(`SELECT * FROM ${tabela}`);
        res.json(rows.rows);
    } catch (error) {
        console.error(`Erro ao buscar dados da tabela ${req.params.tabela}:`, error);
        res.status(500).json({ error: 'Erro ao buscar dados auxiliares' });
    }
});

// Após a definição do pool, adicione:
pool.on('error', (err) => {
    console.error('Erro inesperado no pool de conexão:', err);
});

// Teste a conexão antes de iniciar o servidor
async function startServer() {
    try {
        // Teste de conexão com o banco
        const client = await pool.connect();
        console.log('Conexão com o banco de dados estabelecida com sucesso');
        client.release();

        app.listen(PORT, () => {
            console.log(`Servidor rodando na porta ${PORT}`);
        });
    } catch (error) {
        console.error('Erro ao iniciar o servidor:', error);
        process.exit(1);
    }
}

startServer();