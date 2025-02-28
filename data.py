def generate_episode_data(total_episodes, manual_data=None):
    
    if manual_data:
        return manual_data
    return [{"index": i + 1, "episode": i + 1} for i in range(total_episodes)]


dramas = {
    "Daraar": {
        "link": "https://www.youtube.com/playlist?list=PLdZNFVCDo_1cOWnp-bw3x8CxOw7bMxRt-",
        "episodes": generate_episode_data(40)
    },
    "Baichain Dil": {
        "link": "https://www.youtube.com/playlist?list=PLB1BPYz25JSpGfcskNyX0DmwlXcNOvyT4",
        "episodes": generate_episode_data(37)
    },
    "Main Na Janoo": {
        "link": "https://www.youtube.com/watch?v=5Cun41G44dc&list=PLbVdwtmx18sviyRcmCCQirArY5DR1doQQ&index=34",
        "episodes": generate_episode_data(31)
    },
    "Parizaad": {
        "link": "https://www.youtube.com/watch?v=fwZ6JNfXezg&list=PLbVdwtmx18stXNeBl2fTxbHUsP-HbIYth",
        "episodes": generate_episode_data(29)
    },
    "Qabeel": {
        "link": "https://www.youtube.com/watch?v=4xUvwCzhyQs&list=PLqunGGXHQ5sEsPa8fkFyzvzxUd0e8FRv_&index=1",
        # "link": "https://www.youtube.com/watch?v=_p8bCk8pEv4&list=PLb2aaNHUy_gGbfcGbIOIDbWmpXVpurgGh&index=1",
        "episodes": generate_episode_data(1)
    },
    "Aye Ishq E Junoon": {
        # "link": "https://www.youtube.com/watch?v=1HlBsY_7KOE&list=PLz2MrXbUSiBoaGl0Ia2Q-_G8md6k8DegO",
        "link": "https://www.youtube.com/watch?v=_p8bCk8pEv4&list=PLb2aaNHUy_gGbfcGbIOIDbWmpXVpurgGh&index=1",
        "episodes": generate_episode_data(32)
    },
    "Sotan": {
        "link": "https://www.youtube.com/watch?v=1HlBsY_7KOE&list=PLz2MrXbUSiBoaGl0Ia2Q-_G8md6k8DegO",
        # 
        "episodes": generate_episode_data(58)
    },
    "Zard Patton Ka Bunn": {
        "link": "https://www.youtube.com/watch?v=Y3bPhqTEGSY&list=PLbVdwtmx18su3GY_B7miQbxmhbVh9KTDn",
        # 
        "episodes": generate_episode_data(29)
    },
    "Darlings": {
        # "link": "https://www.youtube.com/watch?v=3ZZn3haoRFA&list=PLs2CG9JU32b7iF3Iszyd63vxm47qeYysE",
        "link": "https://www.youtube.com/watch?v=Gr9UyxQYjO4&list=PLQTepLZOvCg5jD7ljW8Eg2C_HJNvGmicV",
        "episodes": generate_episode_data(55)
    },
    "Kaisa Mera Naseeb": {
        "link": "https://www.youtube.com/watch?v=XI8TJxKc3Kw&list=PLz2MrXbUSiBoojRUSDm1dUi4RdUIDtwXa",
        # "link": "https://www.youtube.com/watch?v=FQxDh-pKXj0&list=PLbVdwtmx18sv59ZlGX7qmAj65AXF5iRNu",
        "episodes": generate_episode_data(8)
    },
    "Akhara": {
        "link": "https://www.youtube.com/watch?v=3ZZn3haoRFA&list=PLs2CG9JU32b7iF3Iszyd63vxm47qeYysE",
        # "link": "https://www.youtube.com/watch?v=soj9FDuHBGU&list=PLeb83ChrfOzkYh3FJFiZ5hW8uZj6yaJ79&index=47",
        "episodes": generate_episode_data(34)
    },
    "Mohabbatain Chahatain": {
        "link": "https://www.youtube.com/watch?v=soj9FDuHBGU&list=PLeb83ChrfOzkYh3FJFiZ5hW8uZj6yaJ79&index=47",
        # "link": "https://www.youtube.com/watch?v=8o7xs7MLpQA&list=PLb2aaNHUy_gHLxFkFX4uFSx-P4vxZ7jBr",
        "episodes": generate_episode_data(6)
    },
    "Jaan Se Pyara Juni": {
        "link": "https://www.youtube.com/watch?v=FQxDh-pKXj0&list=PLbVdwtmx18sv59ZlGX7qmAj65AXF5iRNu",
        "episodes": generate_episode_data(34)
    },
    "Me Kahani Hun": {
        "link": "https://www.youtube.com/watch?v=hLRuSVJ_Ynk&list=PLeb83ChrfOzkFzkenCQthTFLgPB5FsLan&index=12",
        "episodes": generate_episode_data(12)
    },
    "Tere Bina Mein Nahi": {
        "link": "https://www.youtube.com/watch?v=8o7xs7MLpQA&list=PLb2aaNHUy_gHLxFkFX4uFSx-P4vxZ7jBr",
        "episodes": generate_episode_data(39)
    },
    "Umm-e-Haniya": {
        "link": "https://www.youtube.com/watch?v=YxIb_BNJkI0&list=PLdZNFVCDo_1cFNYaFX9C5ZuQ3ZkL3nFGT&index=2",
        "episodes": generate_episode_data(38)
    },
    "Besharam": {
        "link": "https://www.youtube.com/watch?v=kLamSiob72Y&list=PL3y6etwW5z8JxbJp64nA4fmsF_7mgeJai",
        "episodes": generate_episode_data(24)
    },
    
}

# Example usage
print(dramas["Parizaad"]["episodes"])  # Outputs auto-generated episodes
print(dramas["Custom Drama"]["episodes"])  # Outputs manually set episodes
