import express from 'express';
import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

const app = express();
app.use(express.json());

// Initialize the PostgreSQL connection pool
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false,
});

pool.connect()
  .then(() => console.log('Connected to PostgreSQL'))
  .catch(err => {
    console.error('Database connection failed:', err);
    process.exit(1);
  });

// Types for contact and API request/response
interface Contact {
  id: number;
  phoneNumber?: string;
  email?: string;
  linkedId?: number;
  linkPrecedence: 'primary' | 'secondary';
  createdAt: Date;
  updatedAt: Date;
  deletedAt?: Date;
}

interface IdentifyRequest {
  email?: string;
  phoneNumber?: string;
}

interface IdentifyResponse {
  contact: {
    primaryContatctId: number;
    emails: string[];
    phoneNumbers: string[];
    secondaryContactIds: number[];
  };
}

// Create the Contact table if it doesn't exist
async function initializeDatabase() {
  await pool.query(`
    CREATE TABLE IF NOT EXISTS Contact (
      id SERIAL PRIMARY KEY,
      phoneNumber VARCHAR(15),
      email VARCHAR(255),
      linkedId INTEGER REFERENCES Contact(id),
      linkPrecedence VARCHAR(10) CHECK (linkPrecedence IN ('primary', 'secondary')),
      createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updatedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      deletedAt TIMESTAMP
    );
  `);
}

// Fetch contacts based on email or phone
async function getRelatedContacts(email?: string, phoneNumber?: string): Promise<Contact[]> {
  const conditions = [];
  const values: any[] = [];

  if (email) {
    conditions.push(`email = $${values.length + 1}`);
    values.push(email);
  }
  if (phoneNumber) {
    conditions.push(`phoneNumber = $${values.length + 1}`);
    values.push(phoneNumber);
  }

  const whereClause = conditions.length > 0 ? `WHERE deletedAt IS NULL AND (${conditions.join(' OR ')})` : 'WHERE false';
  const result = await pool.query(`SELECT * FROM Contact ${whereClause}`, values);
  return result.rows;
}

// Recursively get the entire contact chain
async function getContactChain(contact: Contact): Promise<Contact[]> {
  const result = await pool.query(`
    WITH RECURSIVE contact_tree AS (
      SELECT * FROM Contact WHERE id = $1 AND deletedAt IS NULL
      UNION ALL
      SELECT c.* FROM Contact c
      INNER JOIN contact_tree ct ON c.linkedId = ct.id
      WHERE c.deletedAt IS NULL
    )
    SELECT * FROM contact_tree ORDER BY createdAt ASC;
  `, [contact.id]);
  return result.rows;
}

async function createContact(email?: string, phoneNumber?: string, linkedId?: number, linkPrecedence: 'primary' | 'secondary' = 'primary'): Promise<Contact> {
  const result = await pool.query(
    `INSERT INTO Contact (phoneNumber, email, linkedId, linkPrecedence, createdAt, updatedAt)
     VALUES ($1, $2, $3, $4, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) RETURNING *`,
    [phoneNumber, email, linkedId, linkPrecedence]
  );
  return result.rows[0];
}

async function updateContactToSecondary(contactId: number, linkedId: number): Promise<void> {
  await pool.query(
    `UPDATE Contact SET linkedId = $1, linkPrecedence = 'secondary', updatedAt = CURRENT_TIMESTAMP WHERE id = $2`,
    [linkedId, contactId]
  );
}

function buildResponse(contacts: Contact[]): IdentifyResponse {
  const primary = contacts.find(c => c.linkPrecedence === 'primary');
  if (!primary) throw new Error('No primary contact found');

  const secondary = contacts.filter(c => c.linkPrecedence === 'secondary');

  const emails = new Set<string>([primary.email!, ...secondary.map(c => c.email!).filter(Boolean)]);
  const phones = new Set<string>([primary.phoneNumber!, ...secondary.map(c => c.phoneNumber!).filter(Boolean)]);

  return {
    contact: {
      primaryContatctId: primary.id,
      emails: Array.from(emails),
      phoneNumbers: Array.from(phones),
      secondaryContactIds: secondary.map(c => c.id)
    },
  };
}

// Main API endpoint
app.post('/identify', async (req, res) => {
  try {
    const { email, phoneNumber }: IdentifyRequest = req.body;
    if (!email && !phoneNumber) return res.status(400).json({ error: 'Email or phoneNumber required' });

    let related = await getRelatedContacts(email, phoneNumber);
    let fullChain = related.length ? await getContactChain(related[0]) : [];

    let primary = fullChain.find(c => c.linkPrecedence === 'primary');
    if (!primary) {
      primary = await createContact(email, phoneNumber, undefined, 'primary');
      return res.json(buildResponse([primary]));
    }

    const alreadyExists = fullChain.some(c => c.email === email && c.phoneNumber === phoneNumber);
    if (!alreadyExists) {
      const existsEmail = fullChain.some(c => c.email === email);
      const existsPhone = fullChain.some(c => c.phoneNumber === phoneNumber);

      if ((email && !existsEmail) || (phoneNumber && !existsPhone)) {
        const secondary = await createContact(email, phoneNumber, primary.id, 'secondary');
        fullChain.push(secondary);
      }
    }

    const primaries = fullChain.filter(c => c.linkPrecedence === 'primary');
    if (primaries.length > 1) {
      const oldest = primaries.reduce((a, b) => new Date(a.createdAt) < new Date(b.createdAt) ? a : b);
      for (const p of primaries) {
        if (p.id !== oldest.id) await updateContactToSecondary(p.id, oldest.id);
      }
    }

    res.json(buildResponse(fullChain));
  } catch (err) {
    console.error('/identify error:', err);
    res.status(500).json({ error: 'Internal server error' ,message:'no primary contacts found' });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Start the server
const PORT = process.env.PORT || 3000;
initializeDatabase()
  .then(() => app.listen(PORT, () => console.log(`Server running on port ${PORT}`)))
  .catch(err => {
    console.error('Initialization failed:', err);
    process.exit(1);
  });

export default app;
