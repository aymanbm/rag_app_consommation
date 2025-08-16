import { BrowserRouter as Router, Routes, Route, NavLink } from 'react-router-dom';
import { useRef, useState } from 'react';
import './App.css';
import Receptions from './components/Receptions.js';
import Mouvements from './components/Mouvements.js';
import Consommations from './components/Consommations.js';

function App() {
  const [activeApp, setActiveApp] = useState('consommation');
  const [question, setQuestion] = useState('');
  const [responseData, setResponseData] = useState();
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

  const buildTableData = (data, type_name) => {
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
      tableData.title = `${type_name} par jour`;
      tableData.headers = ['Date', `${type_name} (tonnes)`, 'Nombre d\'entrées'];

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
        [`${type_name} totale`, formatNumberFR(computed.sum) + ' tonnes'],
        [computed.operation_explanation || 'Résultat de l\'opération', formatNumberFR(computed.operation_result) + ' tonnes']
      ];

    } else if (dateType === 'single') {
      // Single date summary
      tableData.title = `Résumé de la ${type_name}`;
      tableData.headers = ['Métrique', 'Valeur'];
      
      tableData.rows = [
        ['Date', data.debug?.parsed_start || ''],
        ['Famille', data.debug?.detected_family || ''],
        [`${type_name} totale`, formatNumberFR(computed.sum) + ' tonnes'],
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
      tableData.title = `Statistiques de ${type_name}`;
      tableData.headers = ['Métrique', 'Valeur'];
      
      tableData.rows = [
        ['Période', `${data.debug?.parsed_start || ''} - ${data.debug?.parsed_end || ''}`],
        ['Famille', data.debug?.detected_family || ''],
        [`${type_name} totale`, formatNumberFR(computed.sum) + ' tonnes'],
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
      const formName = e.target.name;
      let res;
      if (formName === "consommation") {
          res = await fetch(`${API_URL}/query/consommation`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ question }),
        });
      } else if (formName === "reception") {
          res = await fetch(`${API_URL}/query/reception`, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify({ question }),
        });
      }
      

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

  // const handleActiveApp = (name) => {    
  //   setActiveApp(name);
  //   setResponseData(null);
  // };

  // const tableData = responseData ? buildTableData(responseData, "réception") : null;

  let tableData;
  if (responseData) {
      console.log(responseData);
      
      if (String(responseData.response).toLocaleLowerCase().includes("reception")) {
        tableData = responseData ? buildTableData(responseData, "réception") : null;
        console.log("Response Data:", responseData);
        
      } else if (String(responseData.response).toLocaleLowerCase().includes("consommation")) {        
        tableData = responseData ? buildTableData(responseData, "consommation") : null;
        console.log("Response Data:", responseData);
      }
    }
  return (
    <Router>
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
            {/* Navigation Sidebar */}
            <div className="sidebar">
              {/* <div className="sidebar-header">
                <h3>Applications</h3>
              </div> */}
              <nav className="nav-menu">
                <NavLink 
                  to="/consommation"
                  className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M3 9l9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"></path>
                    <polyline points="9 22 9 12 15 12 15 22"></polyline>
                  </svg>
                  Consommation
                </NavLink>

                <NavLink 
                  to="/receptions"
                  className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <path d="M20 14.66V20a2 2 0 0 1-2 2H4a2 2 0 0 1-2-2V6a2 2 0 0 1 2-2h5.34"></path>
                    <polygon points="18 2 22 6 12 16 8 16 8 12 18 2"></polygon>
                  </svg>
                  Réceptions
                </NavLink>

                {/* <NavLink 
                  to="/mouvement"
                  style={{opacity: 0.5, cursor: 'not-allowed',disabled: true}}
                  className={({ isActive }) => `nav-item ${isActive ? 'desactive' : ''}`}
                >
                  <svg xmlns="http://www.w3.org/2000/svg" width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
                    <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"></polyline>
                  </svg>
                  Mouvement
                </NavLink> */}
              </nav>
            </div>

            {/* Main Content Area */}
            <div className="main-content">
              <Routes>

                {/* Consommation App */}
                <Route path="/consommation" element={
                  <Consommations
                    question={question}
                    setQuestion={setQuestion}
                    handleSubmit={handleSubmit}
                    loading={loading}
                    responseData={responseData}
                    executionTime={executionTime}
                    tableData={tableData}
                  />
                } />

                {/* Réceptions App */}
                <Route path="/receptions" element={
                  <Receptions
                    question={question}
                    setQuestion={setQuestion}
                    handleSubmit={handleSubmit}
                    loading={loading}
                    responseData={responseData}
                    executionTime={executionTime}
                    tableData={tableData}
                  />
                } />

                {/* Mouvement App */}
                <Route path="/mouvement" element={
                  <Mouvements
                    question={question}
                    setQuestion={setQuestion}
                    handleSubmit={handleSubmit}
                    loading={loading}
                    responseData={responseData}
                    executionTime={executionTime}
                    tableData={tableData}
                  />
                }/>
              </Routes>
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
    </Router>
  );
}

export default App;