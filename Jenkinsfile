pipeline {
  agent {
    node {
      label 'python3'
    }
  }
  stages {
    stage('setup') {
      steps {
        echo "Setting up environment..."
        sh '/bin/python36 -m pip install --user -r requirements.txt'
      }
    }

    stage('code-check') {
      steps {
        echo "Checking code with flake8"
        sh '/bin/python36 -m flake8'
      }
    }

    stage('unit-tests') {
      steps {
        echo "Running Nosetests"
        sh '/bin/python36 -m nose'
      }
    }
  }
}
