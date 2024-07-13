import { getCharacters } from 'rickmortyapi';
import fs from 'fs';
import pg from 'pg';

const config = {
    connectionString: "postgres://candidate:62I8anq3cFq5GYh2u4Lh@rc1b-r21uoagjy1t7k77h.mdb.yandexcloud.net:6432/db1",
    ssl: {
        rejectUnauthorized: true,
        ca: fs.readFileSync("./certs/root.crt").toString(),
    },
};

async function setupDatabase() {
    const client = new pg.Client(config);

    try {
        await client.connect();
        console.log("Connected to the database");

        const checkTableQuery = `
            SELECT to_regclass('public.table_name') AS table_exists;
        `;
        const result = await client.query(checkTableQuery);

        if (result.rows[0].table_exists) {
            const dropTableQuery = `
                DROP TABLE public.table_name;
            `;
            await client.query(dropTableQuery);
            console.log("Existing table dropped");
        }

        const createTableQuery = `
            CREATE TABLE public.table_name (
                id SERIAL PRIMARY KEY,
                name TEXT NOT NULL,
                data JSONB
            );
        `;
        await client.query(createTableQuery);
        console.log("Table 'table_name' created");

    } catch (e) {
        console.error("Ошибка подключения:", e);
    } finally {
        await client.end();
        console.log("Disconnected from the database");
    }
}

async function insertCharacters(characters) {
    const client = new pg.Client(config); 

    try {
        await client.connect();
        console.log("Connected to the database");

        for (let character of characters) {
            const insertQuery = `
                INSERT INTO public.table_name (name, data) VALUES ($1, $2);
            `;
            const values = [character.name, JSON.stringify(character)];
            await client.query(insertQuery, values);
        }

        console.log("Данные успешно внесены в базу данных!");

    } catch (e) {
        console.error("Ошибка вставки персонажей:", e);
    } finally {
        await client.end();
        console.log("Disconnected from the database");
    }
}

async function fetchAllCharactersAndInsert() {
    try {
        let page = 1;
        let allCharacters = [];

        while (true) {
            const response = await getCharacters({ page });
            const characters = response.data.results; 

            allCharacters = [...allCharacters, ...characters];

            const names = characters.map(character => character.name);
            console.log(`Извлечение персонажей из страницы: ${page}`);
            console.log('Имена персонажей:', names);

            page++;

            if (!response.data.info.next) {
                break; 
            }
        }
   
        await setupDatabase();
        await insertCharacters(allCharacters); 

        console.log('Таблица создана и данные внесены');
    } catch (e) {
        console.error('Ошибка извлечения или записи персонажей:', e);
    }
}

fetchAllCharactersAndInsert();