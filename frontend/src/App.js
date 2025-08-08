import { useState } from 'react';
import './App.css';

function App() {
  const [question, setQuestion] = useState('');
  const [response, setResponse] = useState('');
  const [executionTime, setExecutionTime] = useState('');
  const [loading, setLoading] = useState(false);
  const [history, setHistory] = useState([]);

  // Use env var if provided, otherwise default to your LAN IP
  const API_URL = process.env.REACT_APP_API_URL || "http://10.4.100.35:8000";

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!question.trim()) return;

    setLoading(true);
    try {
      const res = await fetch(`${API_URL}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question }),
      });

      // If non-2xx, try to read text for debugging and throw
      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status} - ${text}`);
      }

      // Try parsing JSON (server should return JSON)
      let data;
      try {
        data = await res.json();
      } catch (err) {
        // If JSON parsing fails, show raw text
        const raw = await res.text();
        throw new Error(`Invalid JSON from server: ${raw}`);
      }

      // Add to history with execution time
      const newEntry = {
        id: Date.now(),
        question,
        response: data.response,
        executionTime: data.execution_time || data.executionTime || '',
      };
      setHistory([newEntry, ...history.slice(0, 4)]);

      setResponse(data.response);
      setExecutionTime(data.execution_time || data.executionTime || '');
    } catch (error) {
      console.error('Error:', error);
      setResponse(`Erreur: Impossible de traiter votre requête — ${error.message}`);
      setExecutionTime('');
    }
    setLoading(false);
  };

  const clearHistory = () => {
    setHistory([]);
    setResponse('');
    setQuestion('');
  };

  return (
    <div className="App">
      <header className="app-header">
        <div className="header-content">
          <div className="logo-container">
            <div className="logo">
              <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 16V8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16z"></path>
                <polyline points="7.5 4.21 12 6.81 16.5 4.21"></polyline>
                <polyline points="7.5 19.79 7.5 14.6 3 12"></polyline>
                <polyline points="21 12 16.5 14.6 16.5 19.79"></polyline>
                <polyline points="3.27 6.96 12 12.01 20.73 6.96"></polyline>
                <line x1="12" y1="22.08" x2="12" y2="12"></line>
              </svg>
            </div>
            <h1>Système de Consultation de Données</h1>
          </div>
          <div className="header-info">
            <p>Analyse de consommation en temps réel</p>
          </div>
        </div>
      </header>

      <main className="app-container">
        <div className="dashboard">
          <div className="query-section">
            <div className="query-card">
              <div className="card-header">
                <h2>Posez votre question</h2>
                <p className="subtitle">Obtenez des réponses précises à partir de vos données</p>
              </div>

              <form onSubmit={handleSubmit} className="query-form">
                <div className="input-group">
                  <input
                    type="text"
                    value={question}
                    onChange={(e) => setQuestion(e.target.value)}
                    placeholder="Ex: Quelle est la quantité consommée pour la famille MAIS le 11/7/2025 ?"
                    className="query-input"
                    disabled={loading}
                  />
                  <button type="submit" disabled={loading || !question.trim()} className="submit-button">
                    {loading ? (
                        <>
                          <span className="spinner" aria-hidden="true"></span>
                          Traitement...
                        </>
                      ) : (
                        <>
                          <svg className="icon-send" xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true" focusable="false">
                            <line x1="22" y1="2" x2="11" y2="13"></line>
                            <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                          </svg>
                          <span className="btn-label">Envoyer</span>
                        </>
                      )}
                  </button>
                </div>
              </form>

              {response && (
                <div className="response-container">
                  <div className="response-header">
                    <h3>Réponse:</h3>
                    <div className="response-meta">
                      {executionTime && (
                        <div className="time-badge">
                          <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <circle cx="12" cy="12" r="10"/>
                            <polyline points="12 6 12 12 16 14"/>
                          </svg>
                          {executionTime}
                        </div>
                      )}
                      <div className="ai-badge">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                          <path d="M12 8V4H8"></path>
                          <rect x="4" y="4" width="16" height="16" rx="2"></rect>
                          <path d="M2 14h2"></path>
                          <path d="M20 14h2"></path>
                          <path d="M15 13v2"></path>
                          <path d="M9 13v2"></path>
                        </svg>
                        AI Réponse
                      </div>
                    </div>
                  </div>
                  <div className="response-content">
                    <p>{response}</p>
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="history-section">
            <div className="history-card">
              <div className="card-header">
                <h3>Historique des requêtes</h3>
                {history.length > 0 && (
                  <button onClick={clearHistory} className="clear-button">
                    Effacer
                  </button>
                )}
              </div>

              {history.length === 0 ? (
                <div className="empty-history">
                  <svg xmlns="http://www.w3.org/2000/svg" width="48" height="48" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"></path>
                  </svg>
                  <p>Aucun historique</p>
                  <small>Vos requêtes précédentes apparaîtront ici</small>
                </div>
              ) : (
                <div className="history-list">
                  {history.map((item) => (
                    <div key={item.id} className="history-item">
                      <div className="question">
                        <strong>Q:</strong> {item.question}
                      </div>
                      <div className="response">
                        <strong>R:</strong> {item.response}
                        {item.executionTime && (
                          <span className="history-time">
                            ({item.executionTime})
                          </span>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        </div>

        <footer className="app-footer">
          <p>Système de Consultation de Données • Analyse en temps réel • Développé par AYMANE EL MAGHRAOUI</p>
          <div className="footer-links">
            <span>Documentation</span>
            <span>À propos</span>
            <span>Contact</span>
          </div>
        </footer>
      </main>
    </div>
  );
}

export default App;
