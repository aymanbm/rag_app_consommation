// App.js
import { useState } from 'react';
import './App.css';

function App() {
  const [question, setQuestion] = useState('');
  const [responseData, setResponseData] = useState(null);
  const [executionTime, setExecutionTime] = useState('');
  const [loading, setLoading] = useState(false);

  // Use env var if provided, otherwise default to your LAN IP
  const API_URL = process.env.REACT_APP_API_URL || "http://10.4.100.35:8000";

  const formatNumberFR = (num) => {
    if (num === null || num === undefined || Number.isNaN(num)) return '';
    const n = Number(num);
    return n.toLocaleString('fr-FR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  const formatDate = (dateStr) => {
    if (!dateStr) return '';
    try {
      const date = new Date(dateStr);
      return date.toLocaleDateString('fr-FR');
    } catch {
      return dateStr;
    }
  };

  const buildTableData = (data) => {
    if (!data || !data.computed) return null;

    const computed = data.computed;
    const dailyBreakdown = computed.daily_breakdown;
    const dateType = computed.date_type;
    const operation = computed.operation_requested;
    
    // Determine what to show based on the query type and available data
    const hasOperation = operation && operation.op !== 'none' && computed.operation_result !== null;
    const hasDaily = dailyBreakdown && Object.keys(dailyBreakdown).length > 0;

    let tableData = {
      headers: [],
      rows: [],
      title: ''
    };

    if (hasDaily && dateType === 'range') {
      // Daily breakdown table
      tableData.title = 'Consommation par jour';
      tableData.headers = ['Date', 'Consommation (tonnes)', 'Nombre d\'entrées'];
      
      Object.entries(dailyBreakdown)
        .sort(([a], [b]) => new Date(a.split('/').reverse().join('-')) - new Date(b.split('/').reverse().join('-')))
        .forEach(([date, data]) => {
          tableData.rows.push([
            date,
            formatNumberFR(data.total),
            data.entries.toString()
          ]);
        });

      // Add total row
      tableData.rows.push([
        'TOTAL',
        formatNumberFR(computed.sum),
        computed.count.toString()
      ]);

      // Add operation result if present
      if (hasOperation) {
        tableData.rows.push([
          computed.operation_explanation || 'Résultat',
          formatNumberFR(computed.operation_result),
          ''
        ]);
      }

    } else if (hasOperation) {
      // Operation result table
      tableData.title = 'Résultat de l\'opération';
      tableData.headers = ['Métrique', 'Valeur'];
      
      tableData.rows = [
        ['Consommation totale', formatNumberFR(computed.sum) + ' tonnes'],
        [computed.operation_explanation || 'Résultat de l\'opération', formatNumberFR(computed.operation_result) + ' tonnes']
      ];

    } else if (dateType === 'single') {
      // Single date summary
      tableData.title = 'Résumé de la consommation';
      tableData.headers = ['Métrique', 'Valeur'];
      
      tableData.rows = [
        ['Date', data.debug?.parsed_start || ''],
        ['Famille', data.debug?.detected_family || ''],
        ['Consommation totale', formatNumberFR(computed.sum) + ' tonnes'],
        ['Nombre d\'entrées', computed.count.toString()]
      ];

      if (computed.count > 1) {
        tableData.rows.push(
          ['Moyenne par entrée', formatNumberFR(computed.mean) + ' tonnes'],
          ['Minimum', formatNumberFR(computed.min) + ' tonnes'],
          ['Maximum', formatNumberFR(computed.max) + ' tonnes']
        );
      }

    } else {
      // General statistics table
      tableData.title = 'Statistiques de consommation';
      tableData.headers = ['Métrique', 'Valeur'];
      
      tableData.rows = [
        ['Période', `${data.debug?.parsed_start || ''} - ${data.debug?.parsed_end || ''}`],
        ['Famille', data.debug?.detected_family || ''],
        ['Consommation totale', formatNumberFR(computed.sum) + ' tonnes'],
        ['Nombre d\'entrées', computed.count.toString()],
        ['Moyenne', formatNumberFR(computed.mean) + ' tonnes'],
        ['Minimum', formatNumberFR(computed.min) + ' tonnes'],
        ['Maximum', formatNumberFR(computed.max) + ' tonnes']
      ];
    }

    return tableData;
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    if (!question.trim()) return;

    setLoading(true);
    setResponseData(null);
    
    try {
      const res = await fetch(`${API_URL}/query`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ question }),
      });

      if (!res.ok) {
        const text = await res.text();
        throw new Error(`HTTP ${res.status} - ${text}`);
      }

      let data;
      try {
        data = await res.json();
      } catch (err) {
        const raw = await res.text();
        throw new Error(`Invalid JSON from server: ${raw}`);
      }

      setResponseData(data);
      setExecutionTime(data.execution_time || data.executionTime || '');

    } catch (error) {
      console.error('Error:', error);
      setResponseData({
        error: `Erreur: Impossible de traiter votre requête — ${error.message}`
      });
      setExecutionTime('');
    }
    setLoading(false);
  };

  const tableData = responseData ? buildTableData(responseData) : null;

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
                    placeholder="Ex: Quelle est la quantité consommée pour la famille MAIS du 01/06/2024 au 03/06/2024 ?"
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
            </div>
          </div>

          {/* Results Section */}
          {responseData && (
            <div className="results-section">
              <div className="results-card">
                <div className="card-header">
                  <h3>Résultats</h3>
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
                  </div>
                </div>

                {responseData.error ? (
                  <div className="error-message">
                    <svg xmlns="http://www.w3.org/2000/svg" width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                      <circle cx="12" cy="12" r="10"/>
                      <line x1="15" y1="9" x2="9" y2="15"/>
                      <line x1="9" y1="9" x2="15" y2="15"/>
                    </svg>
                    {responseData.error}
                  </div>
                ) : (
                  <>
                    {/* Natural Language Response */}
                    {(responseData.response || responseData.llm_response) && (
                      <div className="natural-response">
                        <div className="response-header">
                          <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                            <path d="M12 8V4H8"></path>
                            <rect x="4" y="4" width="16" height="16" rx="2"></rect>
                            <path d="M2 14h2"></path>
                            <path d="M20 14h2"></path>
                            <path d="M15 13v2"></path>
                            <path d="M9 13v2"></path>
                          </svg>
                          <span>Réponse IA</span>
                        </div>
                        <p className="response-text">
                          {responseData.response || responseData.llm_response}
                        </p>
                      </div>
                    )}

                    {/* Dynamic Data Table */}
                    {tableData && tableData.rows.length > 0 && (
                      <div className="data-table-container">
                        <div className="table-header">
                          <h4>{tableData.title}</h4>
                        </div>
                        <div className="table-wrapper">
                          <table className="data-table">
                            <thead>
                              <tr>
                                {tableData.headers.map((header, index) => (
                                  <th key={index}>{header}</th>
                                ))}
                              </tr>
                            </thead>
                            <tbody>
                              {tableData.rows.map((row, rowIndex) => (
                                <tr key={rowIndex} className={row[0] === 'TOTAL' ? 'total-row' : ''}>
                                  {row.map((cell, cellIndex) => (
                                    <td key={cellIndex} className={cellIndex === 0 ? 'first-col' : ''}>{cell}</td>
                                  ))}
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </>
                )}
              </div>
            </div>
          )}
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