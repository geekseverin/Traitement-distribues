#!/bin/bash

# Setup script for Big Data Project
# Run this on Ubuntu to set up the environment

echo "🚀 Setting up Big Data Environment..."

# Check if Docker is installed
if ! command -v docker &> /dev/null; then
    echo "Installing Docker..."
    sudo apt update
    sudo apt install -y apt-transport-https ca-certificates curl software-properties-common
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"
    sudo apt update
    sudo apt install -y docker-ce
    sudo usermod -aG docker $USER
    echo "Docker installed! Please logout and login again."
fi

# Check if Docker Compose is installed
if ! command -v docker-compose &> /dev/null; then
    echo "Installing Docker Compose..."
    sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
    sudo chmod +x /usr/local/bin/docker-compose
fi

# Create project structure
echo "📁 Creating project structure..."

# Create main directories
mkdir -p docker/hadoop-master/config
mkdir -p docker/hadoop-secondary/config
mkdir -p docker/hadoop-slave/config
mkdir -p docker/mongodb/config
mkdir -p scripts/pig
mkdir -p scripts/setup
mkdir -p applications/streaming-app/templates
mkdir -p data/raw
mkdir -p data/mongodb

# Copy configuration files (you need to create these files first)
echo "📋 Setting up configuration files..."

# Create the docker-compose.yml in root
cat > docker-compose.yml << 'EOF'
# Put the docker-compose.yml content here
EOF

# Create sample data
echo "📊 Creating sample data..."
cat > data/raw/sample_data.csv << 'EOF'
id,name,age,city,salary,department
1,John Doe,28,Paris,45000,IT
2,Jane Smith,32,London,52000,Sales
3,Bob Johnson,45,New York,68000,Finance
4,Alice Brown,29,Berlin,43000,HR
5,Charlie Wilson,38,Tokyo,61000,IT
6,Eva Davis,34,Sydney,55000,Marketing
7,Frank Miller,41,Toronto,59000,Sales
8,Grace Lee,27,Seoul,41000,HR
9,Henry Garcia,36,Madrid,57000,Finance
10,Ivy Wang,31,Beijing,48000,IT
EOF

# Create MongoDB init script
cat > data/mongodb/init-mongodb.js << 'EOF'
# Put the MongoDB init script here
EOF

# Create requirements.txt for Python apps
cat > applications/streaming-app/requirements.txt << 'EOF'
flask==2.3.3
pyspark==3.4.0
pymongo==4.5.0
requests==2.31.0
EOF

# Set permissions
chmod +x scripts/setup/*.sh 2>/dev/null || true

echo "✅ Project structure created!"
echo ""
echo "Next steps:"
echo "1. Copy all the configuration files to their respective directories"
echo "2. Run: docker-compose up -d"
echo "3. Wait for all services to start (check with: docker-compose ps)"
echo "4. Access Hadoop UI at: http://localhost:9870"
echo "5. Access Spark UI at: http://localhost:8080"
echo "6. Run Pig scripts: docker exec namenode pig -f /scripts/pig/data-exploration.pig"
echo ""
echo "🎯 Ready to start your Big Data project!"