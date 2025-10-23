ENERGY_APIS = {
    # US Energy Information Administration
    'eia': 'https://api.eia.gov/v2/',

    # European Network of Transmission System Operators
    'entsoe': 'https://web-api.tp.entsoe.eu/api',

    # Open Power System Data (Germany/Europe)
    'opsd': 'https://data.open-power-system-data.org/time_series/',

    # California ISO (real-time grid data)
    'caiso': 'http://oasis.caiso.com/oasisapi/SingleZip',

    # PJM Interconnection (US Northeast)
    'pjm': 'https://api.pjm.com/api/v1/'
}

# Real transmission interconnections between US RTOs/ISOs
US_GRID_TOPOLOGY = {
    # Eastern Interconnection
    'PJM': {
        'connects_to': ['NYIS', 'MISO', 'CARO'],
        'description': 'Mid-Atlantic hub - largest RTO'
    },
    'NYIS': {
        'connects_to': ['PJM', 'ISNE'],
        'description': 'New York - connects New England to PJM'
    },
    'ISNE': {
        'connects_to': ['NYIS'],
        'description': 'New England - northeastern terminus'
    },
    'MISO': {
        'connects_to': ['PJM', 'SPP'],
        'description': 'Midwest - central hub'
    },
    'SPP': {
        'connects_to': ['MISO', 'ERCO'],  # Limited DC ties to ERCOT
        'description': 'Southwest Power Pool'
    },

    # ERCOT - Mostly isolated!
    'ERCO': {
        'connects_to': ['SPP'],  # Only DC ties, not AC
        'description': 'Texas - electrically isolated grid'
    },

    # Western Interconnection
    'PACW': {
        'connects_to': ['SPP'],  # DC ties
        'description': 'PacifiCorp West - Western grid'
    },

    # Southeastern
    'CARO': {
        'connects_to': ['PJM'],
        'description': 'Carolinas - Duke Energy territory'
    }
}