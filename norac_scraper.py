import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import schedule
import time
from retry import retry

# Database setup
def setup_database():
    conn = sqlite3.connect('norac_projects.db')
    cursor = conn.cursor()
    
    # Create projects table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS projects (
            list_id TEXT PRIMARY KEY,
            title TEXT,
            location TEXT,
            budget TEXT,
            status TEXT,
            description TEXT,
            last_updated TIMESTAMP
        )
    ''')
    
    # Create changes table to track modifications
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS changes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            list_id TEXT,
            field_name TEXT,
            old_value TEXT,
            new_value TEXT,
            change_date TIMESTAMP,
            FOREIGN KEY (list_id) REFERENCES projects (list_id)
        )
    ''')
    
    conn.commit()
    conn.close()

@retry(tries=3, delay=2)
def fetch_projects():
    url = "https://norac.co.ke"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Error fetching projects: {e}")
        raise

def parse_projects(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    projects = []
    
    # Find all property items in the carousel
    property_items = soup.find_all('div', class_='property-item')
    
    for property_item in property_items:
        try:
            # Extract property details
            title = property_item.find('h2', class_='property-title').text.strip()
            price = property_item.find('span', class_='property-price').text.strip()
            
            # Extract location from the description
            description = property_item.find('div', class_='property-element-inline')
            location = description.find('span', class_='property-address').text.strip() if description else ""
            
            # Extract status badge
            status = property_item.find('span', class_='ere__term-status')
            status = status.text.strip() if status else ""
            
            # Extract unique ID from the URL
            property_url = property_item.find('a', class_='property-title-link')
            list_id = property_url['href'].split('/')[-2] if property_url else None
            
            projects.append({
                'list_id': list_id,
                'title': title,
                'location': location,
                'budget': price,
                'status': status,
                'description': description.text.strip() if description else ""
            })
        except Exception as e:
            print(f"Error parsing property: {e}")
            continue
    
    return projects

def save_projects(projects):
    conn = sqlite3.connect('norac_projects.db')
    cursor = conn.cursor()
    
    for project in projects:
        try:
            # Check if project exists
            cursor.execute('SELECT * FROM projects WHERE list_id = ?', (project['list_id'],))
            existing = cursor.fetchone()
            
            # If project doesn't exist, insert it
            if not existing:
                cursor.execute('''
                    INSERT INTO projects 
                    (list_id, title, location, budget, status, description, last_updated)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    project['list_id'],
                    project['title'],
                    project['location'],
                    project['budget'],
                    project['status'],
                    project['description'],
                    datetime.now()
                ))
                print(f"New project added: {project['title']}")
            
            # If project exists, check for changes and update
            else:
                changes = []
                fields = ['title', 'location', 'budget', 'status', 'description']
                
                # Compare each field
                for field in fields:
                    old_value = existing[fields.index(field) + 1]  # +1 to skip list_id
                    new_value = project[field]
                    
                    if old_value != new_value:
                        changes.append({
                            'field_name': field,
                            'old_value': old_value,
                            'new_value': new_value
                        })
                
                # If there are changes, update the project and log them
                if changes:
                    update_fields = ', '.join([f'{field} = ?' for field in fields])
                    values = [project[field] for field in fields]
                    values.extend([project['list_id'], datetime.now()])
                    
                    cursor.execute(f'''
                        UPDATE projects 
                        SET {update_fields}, last_updated = ?
                        WHERE list_id = ?
                    ''', tuple(values))
                    
                    # Log each change
                    for change in changes:
                        cursor.execute('''
                            INSERT INTO changes 
                            (list_id, field_name, old_value, new_value, change_date)
                            VALUES (?, ?, ?, ?, ?)
                        ''', (
                            project['list_id'],
                            change['field_name'],
                            change['old_value'],
                            change['new_value'],
                            datetime.now()
                        ))
                    
                    print(f"Project updated: {project['title']} ({len(changes)} changes)")
                
                else:
                    print(f"No changes detected for: {project['title']}")
                    
        except Exception as e:
            print(f"Error processing project {project['list_id']}: {e}")
            continue
    
    conn.commit()
    conn.close()

def scrape_and_save():
    print(f"Starting scrape at {datetime.now()}")
    try:
        html_content = fetch_projects()
        projects = parse_projects(html_content)
        save_projects(projects)
        print(f"Successfully processed {len(projects)} projects")
    except Exception as e:
        print(f"Error in scrape cycle: {e}")

if __name__ == "__main__":
    # Setup database
    setup_database()
    
    # Schedule hourly scraping
    schedule.every().hour.do(scrape_and_save)
    
    # Run initial scrape
    scrape_and_save()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)
