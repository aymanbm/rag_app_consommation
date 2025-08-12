import Response from "./Response";

const Consommations = (props) => {
  const { question, setQuestion, handleSubmit, loading, 
    responseData, executionTime , tableData
} = props;
  
  return (
    <>
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

            <div className="response-content">
              <Response tableData={tableData} responseData={responseData} />
            </div>
          </div>
        </div>
      )}
    </>
  );
};

export default Consommations;
